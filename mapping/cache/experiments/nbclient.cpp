/*
 * nbclient.cpp
 *
 *  Created on: 03.06.2016
 *      Author: koerberm
 */

#include "datatypes/linecollection.h"
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "cache/experiments/exp_util.h"
#include "cache/experiments/exp_workflows.h"
#include "services/httpparsing.h"
#include "services/ogcservice.h"

#include <mutex>
#include <random>

#include <sys/socket.h>

std::vector<std::unique_ptr<BlockingConnection>> connections;
std::vector<std::unique_ptr<NBClientDeliveryConnection>> del_cons;
std::mutex mtx;
bool done = false;

std::string host = "pc12412.mathematik.uni-marburg.de";
int port = 10042;
//std::string host = "127.0.0.1";
//int port = 12346;



//
// RANDOM STUFF
//
std::random_device rd;
std::mt19937 gen(rd());
std::uniform_real_distribution<> dis;


int next_poisson(int lambda) {
	// Let L ← e−λ, k ← 0 and p ← 1.
	double l = std::exp(-lambda);
	double p = 1;
	int    k = 0;
	do {
		k += 1;
		p *= dis(gen);
	} while (p > l);
	return k-1;
}

std::queue<QTriple> disjoint_queries_from_spec(uint32_t num_queries, const QuerySpec &s,
		uint32_t tiles, uint32_t res) {

	std::uniform_int_distribution<uint16_t> dist(0, tiles * tiles - 1);

	double extend = std::min((s.bounds.x2 - s.bounds.x1) / tiles,
			(s.bounds.y2 - s.bounds.y1) / tiles);

	std::string wf = GenericOperator::fromJSON(s.workflow)->getSemanticId();

	std::queue<QTriple> queries;
	for (size_t i = 0; i < num_queries; i++) {
		uint16_t tile = dist(gen);
		uint16_t y = tile / tiles;
		uint16_t x = tile % tiles;

		double x1 = s.bounds.x1 + x * extend;
		double y1 = s.bounds.y1 + y * extend;

		QueryRectangle qr = s.rectangle(x1, y1, extend, res);
		queries.push(QTriple(CacheType::RASTER, qr, wf));
	}
	return queries;
}

std::queue<QTriple> queries_from_spec(uint32_t num_queries, const QuerySpec &s,
		uint32_t tiles, uint32_t res) {

	double extend = std::min((s.bounds.x2 - s.bounds.x1) / tiles,
			(s.bounds.y2 - s.bounds.y1) / tiles);

	std::string wf = GenericOperator::fromJSON(s.workflow)->getSemanticId();

	std::queue<QTriple> queries;
	for (size_t i = 0; i < num_queries; i++) {
		double x1 = s.bounds.x1 + (s.bounds.x2 - s.bounds.x1 - extend) * dis(gen);
		double y1 = s.bounds.y1 + (s.bounds.y2 - s.bounds.y1 - extend) * dis(gen);
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

			struct linger so_linger;
			so_linger.l_onoff = true;
			so_linger.l_linger = 0;
			setsockopt(con->get_read_fd(),
			    SOL_SOCKET,
			    SO_LINGER,
			    &so_linger,
			    sizeof so_linger);


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
		sleep = std::chrono::milliseconds( next_poisson(inter_arrival) );
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

std::queue<QTriple> btw_queries(int num_queries) {
	return queries_from_spec(num_queries, cache_exp::btw, 64, 512 );
}


class OGCServiceWrapper : public OGCService {
	public:
		using OGCService::OGCService;
		using OGCService::parseEPSG;
		using OGCService::parseTime;
		using OGCService::parseBBOX;
		virtual void run() { throw 1; };
};

std::queue<QTriple> replay_logs(const char *logfile) {
	std::queue<QTriple> queries;

	// instantiate an OGCService for parsing

	// this is an ostream without a streambuf. The library *should* handle that and simply output nothing.
	std::ostream nullstream(nullptr);
	HTTPService::Params _p;
	HTTPService::HTTPResponseStream _hrs(nullptr);
	OGCServiceWrapper ogc(_p, _hrs, nullstream);

	FILE *f = fopen(logfile, "r");
	char line[10000];
	while (fgets(line, 10000, f) != nullptr) {
		auto len = strlen(line);
		if (line[len-1] == '\n')
			line[len-1] = '\0';
		std::string l(line);
		// remove the /cgi-bin/mapping?
		auto pos = l.find_first_of('?');
		if (pos != std::string::npos)
			l = l.substr(pos+1);

		try {
			HTTPService::Params params;
			parseQuery(l, params);

			auto service = params.get("service");
			if (service == "WMS") {
				if (params.get("request") != "GetMap")
					throw ArgumentException("Not GetMap");

				// raster query
				auto query_epsg = ogc.parseEPSG(params, "crs");
				SpatialReference sref = ogc.parseBBOX(params.get("bbox"), query_epsg, false);
				TemporalReference tref = ogc.parseTime(params);

				int output_width = params.getInt("width");
				int output_height = params.getInt("height");
				QueryRectangle qrect(
					sref,
					tref,
					QueryResolution::pixels(output_width, output_height)
				);
				auto graph = GenericOperator::fromJSON(params.get("layers"));

				queries.push(QTriple(CacheType::RASTER, qrect, graph->getSemanticId()));
				//QueryProfiler profiler;
				//auto result_raster = graph->getCachedRaster(qrect,profiler,GenericOperator::RasterQM::LOOSE);
			}
			else if (service == "WFS") {
				auto typeNames = params.get("typenames");
				size_t pos = typeNames.find(":");

				if(pos == std::string::npos)
					throw ArgumentException(concat("WFSService: typeNames delimiter not found", typeNames));

				auto featureTypeString = typeNames.substr(0, pos);
				auto queryString = typeNames.substr(pos + 1);

				TemporalReference tref = ogc.parseTime(params);
				if(!params.hasParam("srsname"))
					throw new ArgumentException("WFSService: Parameter srsname is missing");
				epsg_t queryEpsg = ogc.parseEPSG(params, "srsname");

				SpatialReference sref(queryEpsg);
				if(params.hasParam("bbox")) {
					sref = ogc.parseBBOX(params.get("bbox"), queryEpsg);
				}

				auto graph = GenericOperator::fromJSON(queryString);

				QueryProfiler profiler;

				QueryRectangle rect(
					sref,
					tref,
					QueryResolution::none()
				);

				std::unique_ptr<SimpleFeatureCollection> features;

				CacheType type;
				if (featureTypeString == "points") {
					type = CacheType::POINT;
					//features = graph->getCachedPointCollection(rect, profiler);
				}
				else if (featureTypeString == "lines") {
					type = CacheType::LINE;
					//features = graph->getCachedLineCollection(rect, profiler);
				}
				else if (featureTypeString == "polygons") {
					type = CacheType::POLYGON;
					//features = graph->getCachedPolygonCollection(rect, profiler);
				}
				else
					throw ArgumentException(concat("Unknown featureTypeString in WFS: ", featureTypeString));

				queries.push(QTriple(type, rect, graph->getSemanticId()));
			}
			else if (service == "WCS") {
				continue;
			}
			else
				throw ArgumentException("Unknown service");

			//printf("%s\n", l.c_str());
		}
		catch (const std::exception &e) {
			fprintf(stderr, "Exception parsing query: %s\nError: %s\n", l.c_str(), e.what());
		}
		catch (...) {
//			fprintf(stderr, "Exception parsing query: %s\n", l.c_str());
		}
	}

	return queries;
}

std::queue<QTriple> create_run( int argc, char *argv[] ) {

	std::string workload = argv[2];

	if ( workload == "btw_dis")
		return disjoint_queries_from_spec(30000, cache_exp::btw, 64, 512 );
	else if ( workload == "btw" )
		return queries_from_spec(30000, cache_exp::btw, 64, 512 );
	if ( workload == "srtm_dis")
			return disjoint_queries_from_spec(30000, cache_exp::srtm, 64, 512 );
	else if ( workload == "srtm" )
		return queries_from_spec(30000, cache_exp::srtm, 64, 512 );
	else {
		auto res = replay_logs( workload.c_str() );
		while ( res.size() > 30000 )
			res.pop();
		return res;
	}

}


int main(int argc, char *argv[]) {
	Configuration::loadFromDefaultPaths();
	Log::setLevel(Log::LogLevel::INFO);

	int inter_arrival = atoi(argv[1]);

	auto c = BlockingConnection::create(host, port, true,
			ClientConnection::MAGIC_NUMBER);
	auto rst = c->write_and_read(ClientConnection::CMD_RESET_STATS);

	std::queue<QTriple> qs = create_run(argc,argv);

	std::thread t(issue_queries, &qs, inter_arrival);

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

