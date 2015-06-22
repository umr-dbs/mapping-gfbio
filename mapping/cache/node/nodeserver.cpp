/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/node/nodeserver.h"
#include "cache/index/indexserver.h"
#include "cache/priv/transfer.h"
#include "cache/cache.h"
#include "raster/exceptions.h"
#include "util/make_unique.h"
#include <sstream>

#include <sys/select.h>
#include <sys/socket.h>

////////////////////////////////////////////////////////////
//
// DELIVERY MANAGER
//
////////////////////////////////////////////////////////////

DeliveryManager::DeliveryManager(uint32_t listen_port) :
		shutdown(false), listen_port(listen_port), delivery_id(1) {
}


uint64_t DeliveryManager::add_delivery(std::unique_ptr<GenericRaster>& result) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	uint64_t res = delivery_id++;
	deliveries[res] = std::move(result);
	Log::trace("Added delivery with id: %d", res);
	return res;
}

std::unique_ptr<GenericRaster> DeliveryManager::get_delivery(uint64_t id) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	Log::trace("Getting delivery with id: %d", id);
	auto &gr = deliveries.at(id);
	std::unique_ptr<GenericRaster> res = std::move(gr);
	deliveries.erase(id);
	return res;
}

void DeliveryManager::process_delivery(uint8_t cmd, SocketConnection& con) {
	switch (cmd) {
		case Common::CMD_DELIVERY_GET: {
			uint64_t id;
			con.stream->read(&id);
			try {
				auto res = get_delivery(id);
				Log::debug("Sending delivery: %d", id);
				uint8_t resp = Common::RESP_DELIVERY_OK;
				con.stream->write(resp);
				res->toStream(*con.stream);
				Log::debug("Finished sending delivery: %d", id);
			} catch (std::out_of_range &oor) {
				Log::info("Received request for unknown delivery-id: %d", id);
				uint8_t resp = Common::RESP_DELIVERY_ERROR;
				std::ostringstream msg;
				msg << "Invalid delivery id: " << id;
				con.stream->write(resp);
				con.stream->write(msg.str());
			}
			break;
		}
		case Common::CMD_DELIVERY_GET_CACHED_RASTER: {
			STCacheKey key(*con.stream);
			try {
				Log::debug("Sending cache-entry: %s:%d", key.semantic_id.c_str(), key.entry_id);
				auto res = CacheManager::getInstance().get_raster(key);
				uint8_t resp = Common::RESP_DELIVERY_OK;
				con.stream->write(resp);
				res->toStream(*con.stream);
				Log::debug("Finished sending cache-entry: %s:%d", key.semantic_id.c_str(), key.entry_id);
			} catch (NoSuchElementException &nse) {
				uint8_t resp = Common::RESP_DELIVERY_ERROR;
				std::ostringstream msg;
				msg << "No cache-entry found for key: " << key.semantic_id << ":" << key.entry_id;
				con.stream->write(resp);
				con.stream->write(msg.str());
			}
			break;
		}
		default:
			// Unknown command
			std::ostringstream ss;
			ss << "Unknown command on delivery connection: " << cmd << ". Dropping connection.";
			throw NetworkException(ss.str());
	}
}

void DeliveryManager::run() {
	Log::info("Starting Delivery-Manager");
	int delivery_fd = Common::get_listening_socket(listen_port);

	// Read on delivery-socket
	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(delivery_fd, &readfds);

		int maxfd = delivery_fd;

		for (auto &dc : connections) {
			FD_SET(dc->fd, &readfds);
			maxfd = std::max(maxfd, dc->fd);
		}

		int ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (ret <= 0)
			continue;

		// New delivery connection
		if (FD_ISSET(delivery_fd, &readfds)) {
			struct sockaddr_storage remote_addr;
			socklen_t sin_size = sizeof(remote_addr);
			int new_fd = accept(delivery_fd, (struct sockaddr *) &remote_addr, &sin_size);
			if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
				Log::error("Accept failed: %d", strerror(errno));
			}
			else if (new_fd > 0) {
				Log::debug("New delivery-connection established on fd: %d", new_fd);
				connections.push_back(std::make_unique<SocketConnection>(new_fd));
			}
		}

		// Action on delivery connections
		auto dciter = connections.begin();
		uint8_t cmd;
		while (dciter != connections.end()) {
			auto &dc = *dciter;
			if (FD_ISSET(dc->fd, &readfds)) {
				try {
					if (dc->stream->read(&cmd, true)) {
						process_delivery(cmd, *dc);
						++dciter;
					}
					else {
						Log::debug("Delivery-connection closed on fd: %d", dc->fd);
						dciter = connections.erase(dciter);
					}

				} catch (NetworkException &ne) {
					Log::error("Error on delivery-connection with fd: %d. Dropping. Reason: %s", dc->fd,
							ne.what());
					dciter = connections.erase(dciter);
				}
			}
			else
				++dciter;

		}
	}

	Log::info("Delivery-Manager done.");
}

std::unique_ptr<std::thread> DeliveryManager::run_async() {
	return std::make_unique<std::thread>(&DeliveryManager::run, this);
}

void DeliveryManager::stop() {
	Log::info("Delivery-manager shutting down.");
	shutdown = true;
}

DeliveryManager::~DeliveryManager() {
	stop();
}

////////////////////////////////////////////////////////////
//
// NODE SERVER
//
////////////////////////////////////////////////////////////

NodeServer::NodeServer(std::string my_host, uint32_t my_port, std::string index_host, uint32_t index_port,
		int num_threads) :
		shutdown(false), workers_up(false), my_id(-1), my_host(my_host), my_port(my_port), index_host(
				index_host), index_port(index_port), num_treads(num_threads), delivery_manager(my_port) {
}

void NodeServer::worker_loop() {
	while (workers_up && !shutdown) {
		try {
			Log::debug("Worker connecting to index-server");
			SocketConnection sc(index_host.c_str(), index_port);
			uint8_t cmd = Common::CMD_INDEX_REGISTER_WORKER;
			sc.stream->write(cmd);
			sc.stream->write(my_id);

			CacheManager::remote_connection = &sc;

			while (workers_up && !shutdown) {
				try {
					uint8_t cmd;
					if (Common::read(&cmd, sc, 2, true)) {
						process_worker_command(cmd, sc);
					}
					else {
						Log::info("Disconnect on worker.");
						break;
					}
				} catch (TimeoutException &te) {
					//Log::trace("Read on worker-connection timed out. Trying again");
				} catch (InterruptedException &ie) {
					Log::info("Read on worker-connection interrupted. Trying again.");
				} catch (NetworkException &ne ) {
					// Re-throw network-error to outer catch.
					throw;
				} catch ( std::exception &e ) {
					std::ostringstream os;
					os << "Unexpected error while processing request: " << e.what();
					uint8_t resp = Common::RESP_WORKER_ERROR;
					std::string msg = os.str();
					sc.stream->write(resp);
					sc.stream->write(msg);
				}
			}
		} catch (NetworkException &ne) {
			Log::info("Worker lost connection to index... Reconnecting. Reason: %s", ne.what());
		}
	}
	Log::info("Worker done.");
}

void NodeServer::process_worker_command(uint8_t cmd, SocketConnection& con) {
	Log::debug("Received command: %d", cmd);
	switch (cmd) {
		case Common::CMD_WORKER_CREATE_RASTER: {
			RasterBaseRequest rr(*con.stream);
			QueryProfiler profiler;
			Log::debug("Processing request: %s", rr.to_string().c_str());
			std::unique_ptr<GenericRaster> res = GenericOperator::fromJSON(rr.semantic_id)->getCachedRaster(
					rr.query, profiler, rr.query_mode);
			Log::debug("Handing raster over to delivery-manager");
			uint64_t delivery_id = delivery_manager.add_delivery(res);
			Log::debug("Sending response");
			uint8_t resp = Common::RESP_WORKER_RESULT_READY;
			con.stream->write(resp);
			con.stream->write(delivery_id);
			break;
		}
		case Common::CMD_WORKER_DELIVER_RASTER: {
			RasterDeliveryRequest rr(*con.stream);
			Log::debug("Processing request: %s", rr.to_string().c_str());
			auto result = CacheManager::getInstance().get_raster(rr.semantic_id,rr.entry_id);
			if ( rr.query_mode == GenericOperator::RasterQM::EXACT ) {
				result = result->fitToQueryRectangle(rr.query);
			}
			Log::debug("Handing raster over to delivery-manager");
			uint64_t delivery_id = delivery_manager.add_delivery(result);
			Log::debug("Sending response");
			uint8_t resp = Common::RESP_WORKER_RESULT_READY;
			con.stream->write(resp);
			con.stream->write(delivery_id);
			break;
		}
		case Common::CMD_WORKER_PUZZLE_RASTER: {
			RasterPuzzleRequest rr(*con.stream);
			Log::debug("Processing request: %s", rr.to_string().c_str());
			std::unique_ptr<GenericRaster> res = Common::process_raster_puzzle(rr,my_host,my_port);
			Log::debug("Adding puzzled raster to cache.");
			CacheManager::getInstance().put_raster(rr.semantic_id, res);

			if ( rr.query_mode == RasterPuzzleRequest::QM::EXACT ) {
				Log::debug("Fitting result to query.");
				res = res->fitToQueryRectangle(rr.query);
			}
			Log::debug("Handing raster over to delivery-manager");
			uint64_t delivery_id = delivery_manager.add_delivery(res);
			Log::debug("Sending response");
			uint8_t resp = Common::RESP_WORKER_RESULT_READY;
			con.stream->write(resp);
			con.stream->write(delivery_id);
			break;
		}
		default: {
			Log::error("Unknown command from index-server: %d. Dropping connection.", cmd);
			throw NetworkException("Unknown command from index-server");
		}
	}
	Log::debug("Finished processing command: %d", cmd);
}

void NodeServer::run() {
	Log::info("Starting Node-Server");

	delivery_thread = delivery_manager.run_async();

	while (!shutdown) {
		try {
			setup_control_connection();

			workers_up = true;
			for (int i = 0; i < num_treads; i++)
				workers.push_back(std::make_unique<std::thread>(&NodeServer::worker_loop, this));

			// Read on control
			while (!shutdown) {
				try {
					uint8_t cmd;
					if (Common::read(&cmd, *control_connection, 2, true)) {
						process_control_command(cmd);
					}
					else {
						Log::info("Disconnect on control-connection. Reconnecting.");
						break;
					}
				} catch (TimeoutException &te) {
					//Log::trace("Timeout on read from control-connection.");

				} catch (InterruptedException &ie) {
					Log::info("Interrupt on read from control-connection.");

				} catch (NetworkException &ne) {
					Log::error("Error reading on control-connection. Reconnecting");
					break;
				}
			}
			workers_up = false;
			Log::debug("Waiting for worker-threads to terminate.");
			for (auto &w : workers) {
				w->join();
			}
			workers.clear();
		} catch (NetworkException &ne_c) {
			Log::warn("Could not connect to index-server. Retrying in 5s. Reason: %s", ne_c.what());
			std::this_thread::sleep_for(std::chrono::seconds(5));
		}
	}
	delivery_manager.stop();
	delivery_thread->join();

	Log::info("Node-Server done.");
}

void NodeServer::process_control_command(uint8_t cmd) {
	(void) cmd;
}

void NodeServer::setup_control_connection() {
	Log::info("Connecting to index-server: %s:%d", index_host.c_str(), index_port);

	// Establish connection
	this->control_connection.reset(new SocketConnection(index_host.c_str(), index_port));

	Log::debug("Sending hello to index-server");
	// Say hello
	uint8_t cmd = Common::CMD_INDEX_NODE_HELLO;
	this->control_connection->stream->write(cmd);
	this->control_connection->stream->write(my_host);
	this->control_connection->stream->write(my_port);

	Log::debug("Waiting for response from index-server");
	// Read node-id
	uint8_t resp;
	this->control_connection->stream->read(&resp);
	if (resp == Common::RESP_INDEX_NODE_HELLO) {
		this->control_connection->stream->read(&my_id);
		Log::info("Successfuly connected to index-server. My Id is: %d", my_id);
	}
	else {
		std::ostringstream msg;
		msg << "Index returned unknown response-code to: " << resp << ".";
		throw NetworkException(msg.str());
	}
}

std::unique_ptr<std::thread> NodeServer::run_async() {
	return std::make_unique<std::thread>(&NodeServer::run, this);
}

void NodeServer::stop() {
	Log::info("Node-server shutting down.");
	shutdown = true;
}

NodeServer::~NodeServer() {
	stop();
}