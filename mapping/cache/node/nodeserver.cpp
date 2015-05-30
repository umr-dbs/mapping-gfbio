/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/node/nodeserver.h"
#include "cache/index/indexserver.h"
#include "raster/exceptions.h"
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
;

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
	case Common::CMD_DELIVERY_GET:
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
	default:
		// Unknown command
		std::ostringstream ss;
		ss << "Unknown command on delivery connection: " << cmd << ". Dropping connection.";
		throw NetworkException(ss.str());
	}
}

void DeliveryManager::run() {
	Log::info("Starting Delivery-Manager");
	int delivery_fd = Common::getListeningSocket(listen_port);

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
				connections.push_back(std::unique_ptr<SocketConnection>(new SocketConnection(new_fd)));
			}
		}

		// Action on delivery connections
		auto dciter = connections.begin();
		uint8_t cmd;
		while (dciter != connections.end()) {
			auto &dc = *dciter;
			if (FD_ISSET(dc->fd, &readfds)) {
				try {
					if ( dc->stream->read(&cmd, true ) ) {
						process_delivery(cmd, *dc);
						++dciter;
					}
					else {
						Log::debug("Delivery-connection closed on fd: %d", dc->fd);
						dciter = connections.erase(dciter);
					}

				} catch (NetworkException &ne) {
					Log::error("Error on delivery-connection with fd: %d. Dropping. Reason: %s", dc->fd, ne.what());
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
	return std::unique_ptr<std::thread>(new std::thread(&DeliveryManager::run, this));
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

			while (workers_up && !shutdown) {
				try {
					uint8_t cmd;
					Log::trace("Worker waiting for command.");

					if (Common::read(&cmd, sc, 2, true)) {
						process_worker_command(cmd, sc);
					}
					else {
						Log::info("Disconnect on worker.");
						break;
					}
				} catch (TimeoutException &te) {
					Log::trace("Read on worker-connection timed out. Trying again");
				} catch (InterruptedException &ie) {
					Log::info("Read on worker-connection interrupted. Trying again.");
				}
			}
		} catch (NetworkException &ne) {
			Log::info("Worker lost connection to index... Retrying. Reason: %s", ne.what());
		}
	}
	Log::info("Worker done.");
}

void NodeServer::process_worker_command(uint8_t cmd, SocketConnection& con) {
	Log::debug("Processing command: %d", cmd);
	switch (cmd) {
	case Common::CMD_WORKER_GET_RASTER:
		Log::trace("Reading raster-request.");
		std::string graph_str;
		uint8_t mode;
		QueryProfiler profiler;
		con.stream->read(&graph_str);
		QueryRectangle qr(*con.stream);
		con.stream->read(&mode);
		Log::trace("Fetching raster from operator-graph");
		std::unique_ptr<GenericRaster> res = GenericOperator::fromJSON(graph_str)->getCachedRaster(qr,
				profiler, mode == 1 ? GenericOperator::RasterQM::EXACT : GenericOperator::RasterQM::LOOSE);
		Log::trace("Handing rater over to delivery-manager");
		uint64_t delivery_id = delivery_manager.add_delivery(res);
		Log::trace("Sending response");
		uint8_t resp = Common::RESP_WORKER_RESULT_READY;
		con.stream->write(resp);
		con.stream->write(delivery_id);
		break;
	}
	Log::debug("Finished processing command: %d", cmd);
}

void NodeServer::run() {
	Log::info("Starting Node-Server");

	auto delivery_thread = delivery_manager.run_async();

	while (!shutdown) {
		try {
			setup_control_connection();

			workers_up = true;
			for (int i = 0; i < num_treads; i++)
				workers.push_back(
						std::unique_ptr<std::thread>(new std::thread(&NodeServer::worker_loop, this)));

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
					Log::trace("Timeout on read from control-connection.");

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
	return std::unique_ptr<std::thread>(new std::thread(&NodeServer::run, this));
}

void NodeServer::stop() {
	Log::info("Node-server shutting down.");
	shutdown = true;
}

NodeServer::~NodeServer() {
	stop();
}

