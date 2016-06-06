/*
 * nbclient.cpp
 *
 *  Created on: 03.06.2016
 *      Author: koerberm
 */

#include "cache/experiments/exp_util.h"
#include "cache/experiments/exp_workflows.h"
#include <mutex>

std::vector<std::unique_ptr<BlockingConnection>> connections;
std::vector<std::unique_ptr<NBClientDeliveryConnection>> del_cons;
std::mutex mtx;
bool done = false;

std::string host = "127.0.0.1";
int port = 12346;

std::queue<QTriple> queries_from_spec(uint32_t num_queries, const QuerySpec &s,
		uint32_t tiles, uint32_t res) {

	std::default_random_engine eng(
			std::chrono::system_clock::now().time_since_epoch().count());
	std::uniform_int_distribution<uint16_t> dist(0, tiles * tiles - 1);

	double extend = std::min((s.bounds.x2 - s.bounds.x1) / tiles,
			(s.bounds.y2 - s.bounds.y1) / tiles);

	std::string wf = GenericOperator::fromJSON(s.workflow)->getSemanticId();

	std::queue<QTriple> queries;
	for (size_t i = 0; i < num_queries; i++) {
		uint16_t tile = dist(eng);
		uint16_t y = tile / tiles;
		uint16_t x = tile % tiles;

		double x1 = s.bounds.x1 + x * extend;
		double y1 = s.bounds.y1 + y * extend;

		QueryRectangle qr = s.rectangle(x1, y1, extend, res);
		queries.push(QTriple(CacheType::RASTER, qr, wf));
	}
	return queries;
}

void issue_queries(std::queue<QTriple> *queries, int inter_arrival) {

	auto sleep = std::chrono::milliseconds(0);

	Log::info("Posing %lu queries with %dms inter-arrival time.",
			queries->size(), inter_arrival);

	while (!queries->empty()) {
		try {
			std::this_thread::sleep_for(sleep);
			std::unique_ptr<BlockingConnection> con =
					BlockingConnection::create(host, port, true,
							ClientConnection::MAGIC_NUMBER);
			auto &q = queries->front();
			con->write(ClientConnection::CMD_GET,
					BaseRequest(q.type, q.semantic_id, q.query));
			{
				std::lock_guard<std::mutex> guard(mtx);
				connections.push_back(std::move(con));
			}
			queries->pop();
		} catch (const NetworkException &ex) {
			Log::error("Issuing request failed: %s", ex.what());
		}
		sleep = std::chrono::milliseconds(inter_arrival);
	}
	done = true;
	Log::info("Finished posing queries.");

}

int setup_fdset(fd_set *readfds) {
	int maxfd = 0;

	auto it = del_cons.begin();
	while (it != del_cons.end()) {
		auto &c = **it;
		if (c.is_faulty())
			it = del_cons.erase(it);
		else {
			FD_SET(c.get_read_fd(), readfds);
			maxfd = std::max(maxfd, c.get_read_fd());
			it++;
		}
	}

	std::lock_guard<std::mutex> guard(mtx);
	for (auto &c : connections) {
		FD_SET(c->get_read_fd(), readfds);
		maxfd = std::max(maxfd, c->get_read_fd());
	}
	return maxfd;
}

void process_connections(fd_set *readfds) {
	std::lock_guard<std::mutex> guard(mtx);
	auto it = connections.begin();
	while (it != connections.end()) {
		auto &c = **it;
		try {
			if (FD_ISSET(c.get_read_fd(), readfds)) {
				auto resp = c.read();
				uint8_t rc = resp->read<uint8_t>();
				switch (rc) {
				case ClientConnection::RESP_OK: {
					DeliveryResponse dr(*resp);
					Log::debug("Revceived response: %s", dr.to_string().c_str());
					del_cons.push_back(NBClientDeliveryConnection::create(dr));
					break;
				}
				case ClientConnection::RESP_ERROR: {
					std::string msg = resp->read<std::string>();
					Log::debug("Received error for request: %s", msg.c_str());
					break;
				}
				default:
					throw IllegalStateException("Illegal response from index.");
				}
				it = connections.erase(it);
			} else
				it++;
		} catch (const std::exception &ex) {
			Log::error("Error reading response: %s", ex.what());
			it = connections.erase(it);
		}
	}
}

void process_del_cons(fd_set *readfds) {
	auto it = del_cons.begin();
	while (it != del_cons.end()) {
		auto &c = **it;
		try {
			if ( FD_ISSET(c.get_read_fd(),readfds) && c.input()) {
				Log::debug("Delivery swallowed!");
				it = del_cons.erase(it);
			} else
				it++;
		} catch (const std::exception &ex) {
			Log::error("Error reading response: %s", ex.what());
			it = del_cons.erase(it);
		}
	}
}

int main(void) {
	Configuration::loadFromDefaultPaths();
	Log::setLevel(Log::LogLevel::INFO);

	auto qs = queries_from_spec(10000, cache_exp::srtm, 32, 256);

	QueryRectangle qr = cache_exp::srtm.random_rectangle_percent(0.0625, 256);
	std::queue<QTriple> qs2;

	std::string wf =
			GenericOperator::fromJSON(cache_exp::srtm.workflow)->getSemanticId();

	qs2.push(QTriple(cache_exp::srtm.type, qr, wf));
	qs2.push(QTriple(cache_exp::srtm.type, qr, wf));

	int inter_arrival = 500;

	auto c = BlockingConnection::create(host, port, true,
			ClientConnection::MAGIC_NUMBER);
	auto rst = c->write_and_read(ClientConnection::CMD_RESET_STATS);

	std::thread t(issue_queries, &qs2, inter_arrival);

//	if ( done && connections.empty() && del_cons.empty() )
	while (!done || !connections.empty() || !del_cons.empty()) {
		if (connections.empty() && del_cons.empty()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			continue;
		}

		struct timeval tv { 1, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);

		int maxfd = setup_fdset(&readfds);

		int sel_ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		} else if (sel_ret > 0) {
			process_connections(&readfds);
			process_del_cons(&readfds);
		}
	}
	Log::info("Processing finished. Requesting stats.");
	t.join();
	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto resp = c->write_and_read(ClientConnection::CMD_GET_STATS);

	// Consume code
	resp->read<uint8_t>();
	SystemStats ss(*resp);

	Log::info("System-stats: %s", ss.to_string().c_str());

	return 0;
}

