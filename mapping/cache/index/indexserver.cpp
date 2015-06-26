/*
 * indexserver.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "util/make_unique.h"

#include <deque>
#include <unordered_map>
#include <memory>

#include <stdlib.h>
#include <stdio.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>
#include <errno.h>
#include <cstring>

////////////////////////////////////////////////////////////
//
// NODE
//
////////////////////////////////////////////////////////////

Node::Node(uint32_t id, const std::string &host, uint32_t port) :
	id(id), host(host), port(port) {
}

////////////////////////////////////////////////////////////
//
// INDEX SERVER
//
////////////////////////////////////////////////////////////

IndexServer::IndexServer(int port) :
	port(port), shutdown(false), next_node_id(1) {
}

IndexServer::~IndexServer() {
}

void IndexServer::stop() {
	Log::info("Shutting down.");
	shutdown = true;
}

void IndexServer::run() {
	int listen_socket = Common::get_listening_socket(port);
	Log::info("index-server: listening on node-port: %d", port);

	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		// Add listen sockets
		FD_SET(listen_socket, &readfds);

		int maxfd = listen_socket;

		for (auto &e : worker_connections) {
			handle_worker_connection(*e.second);
			FD_SET(e.second->get_read_fd(), &readfds);
			maxfd = std::max(maxfd, e.second->get_read_fd());
		}

		for (auto &e : control_connections) {
			handle_control_connection(*e.second);
			FD_SET(e.second->get_read_fd(), &readfds);
			maxfd = std::max(maxfd, e.second->get_read_fd());
		}

		for (auto &e : client_connections) {
			handle_client_connection(*e.second);
			FD_SET(e.second->get_read_fd(), &readfds);
			maxfd = std::max(maxfd, e.second->get_read_fd());
		}

		int sel_ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		}
		else if (sel_ret > 0) {
			read_worker_connections(&readfds);
			read_control_connections(&readfds);
			read_client_connections(&readfds);
			process_accept(listen_socket, &readfds);
		}
	}
	close(listen_socket);
	Log::info("Index-Server done.");
}

void IndexServer::process_accept(int listen_socket, fd_set* readfds) {

	if (FD_ISSET(listen_socket, readfds)) {
		struct sockaddr_storage remote_addr;
		socklen_t sin_size = sizeof(remote_addr);
		int new_fd = accept(listen_socket, (struct sockaddr *) &remote_addr, &sin_size);
		if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
			Log::error("Accept failed: %d", strerror(errno));
		}
		else if (new_fd > 0) {
			try {
				Log::debug("New connection established on fd: %d", new_fd);
				std::unique_ptr<UnixSocket> us = std::make_unique<UnixSocket>(new_fd, new_fd);
				BinaryStream &s = *us;

				uint32_t magic;
				s.read(&magic);
				switch (magic) {
					case ClientConnection::MAGIC_NUMBER: {
						std::unique_ptr<ClientConnection> cc = std::make_unique<ClientConnection>(us);
						client_connections.emplace(cc->id, std::move(cc));
						break;
					}
					case WorkerConnection::MAGIC_NUMBER: {
						uint32_t node_id;
						s.read(&node_id);
						std::unique_ptr<WorkerConnection> wc = std::make_unique<WorkerConnection>(us, node_id,
							raster_cache, nodes);
						worker_connections.emplace(wc->id, std::move(wc));
						break;
					}
					case ControlConnection::MAGIC_NUMBER: {
						std::string host;
						uint32_t port;
						s.read(&host);
						s.read(&port);
						NP node = std::make_unique<Node>(next_node_id++, host, port);
						std::unique_ptr<ControlConnection> cc = std::make_unique<ControlConnection>(us, node);
						Log::info("New node registered. ID: %d, control-connected fd: %d", node->id,
							cc->get_read_fd());
						control_connections.emplace(cc->id, std::move(cc));
						nodes.emplace(node->id, node);
						break;
					}
					default: {
						Log::warn("Received unknown magic-number: %d. Dropping connection.", magic);
					}
				}
			} catch (std::exception &e) {
				Log::error("Error on new connection: %s. Dropping.", e.what());
			}
		}
	}
}

void IndexServer::handle_control_connection(ControlConnection& cc) {
	(void) cc;
	// Nothing yet
}

void IndexServer::read_control_connections(fd_set* readfds) {
	auto it = control_connections.begin();
	while (it != control_connections.end()) {
		ControlConnection &cc = *it->second;
		try {
			if (FD_ISSET(cc.get_read_fd(), readfds))
				cc.input();
			it++;
		} catch (NetworkException &ne) {
			// TODO: Drop all connections and remove node
			control_connections.erase(it++);
		}
	}
}

void IndexServer::handle_worker_connection(WorkerConnection& wc) {
	try {
		switch (wc.get_state()) {
			case WorkerConnection::State::DONE: {
				Log::debug("Worker finished Job. Sending result to client.");
				auto &cc = *client_connections.at(wc.get_client_id());
				cc.send_response(wc.get_result());
				wc.reset();
				break;
			}
			case WorkerConnection::State::ERROR: {
				Log::debug("Worker reported error while Job: %s. Forwarding to client.",
					wc.get_error_message().c_str());
				auto &cc = *client_connections.at(wc.get_client_id());
				cc.send_error(wc.get_error_message());
				wc.reset();
				break;
			}
			default: {
				Log::trace("Worker in idle or processing-state. Nothing to do.");
			}
		}
	} catch (NetworkException &ne) {
		Log::error("Could not respond to client: %s. Dropping client connection.", ne.what());
		client_connections.erase(wc.get_client_id());
		wc.reset();
	} catch (IllegalStateException &ise) {
		Log::error("Client is in illegal state. Dropping client connections.");
		client_connections.erase(wc.get_client_id());
		wc.reset();
	} catch (std::out_of_range &oor) {
		Log::error("Client-connection does not exist: %d", wc.get_client_id());
		wc.reset();
	}
}

void IndexServer::read_worker_connections(fd_set* readfds) {
	auto it = worker_connections.begin();
	while (it != worker_connections.end()) {
		WorkerConnection &wc = *it->second;
		try {
			if (FD_ISSET(wc.get_read_fd(), readfds))
				wc.input();
			it++;
		} catch (NetworkException &ne) {
			Log::info("Unexpected error on worker-connection %d: %s", wc.id, ne.what());
			// Redestribute job
			if (wc.get_state() == WorkerConnection::State::PROCESSING) {
				Log::info("Rescheduling job of errorous worker %d", wc.id);
				try {
					client_connections.at(wc.get_client_id())->retry();
				} catch (IllegalStateException &ise) {
					Log::error("Could retry client request: %s. Dropping client connection.", ne.what());
					client_connections.erase(wc.get_client_id());
				} catch (std::out_of_range &oor) {
					Log::error("Retry failed. Client-connection does not exist: %d", wc.get_client_id());
				}
			}
			worker_connections.erase(it++);
		} catch (IllegalStateException &is) {
			Log::info("Worker sent data in illegal state. Dropping.");
			worker_connections.erase(it++);
		}
	}
}

void IndexServer::handle_client_connection(ClientConnection& cc) {
	try {
		switch (cc.get_state()) {
			case ClientConnection::State::REQUEST_READ: {
				switch (cc.get_request_type()) {
					case ClientConnection::RequestType::RASTER: {
						process_raster_request (cc);
						break;
					}
					default: {
						Log::error("Unimplemented request-type. Sending error");
						cc.send_error("Unimplemented request-type");
					}
				}
				break;
			}
			default: {
				Log::trace("Client in idle or processing-state. Nothing todo");
			}
		}
	} catch (NoSuchElementException &nse) {
		Log::debug("No worker available for processing request. Trying again later.");
	}
}

void IndexServer::read_client_connections(fd_set* readfds) {
	auto it = client_connections.begin();
	while (it != client_connections.end()) {
		ClientConnection &cc = *it->second;
		try {
			if (FD_ISSET(cc.get_read_fd(), readfds))
				cc.input();
			it++;
		} catch (NetworkException &ne) {
			client_connections.erase(it++);
		} catch (IllegalStateException &is) {
			client_connections.erase(it++);
		}
	}
}

void IndexServer::process_raster_request(ClientConnection& con) {
	const RasterBaseRequest &req = con.get_raster_request();
	Log::debug("Querying raster-cache for: %s::%s", req.semantic_id.c_str(),
		Common::qr_to_string(req.query).c_str());

	STQueryResult res = raster_cache.query(req.semantic_id, req.query);

	Log::debug("QueryResult: %s", res.to_string().c_str());

	uint64_t worker = 0;
	uint8_t j_cmd = 0;
	std::unique_ptr<BaseRequest> w_req;

	// Full single hit
	if (res.ids.size() == 1 && !res.has_remainder()) {
		Log::debug("Creating raster-delivery job.");
		auto ref = raster_cache.get(req.semantic_id, res.ids[0]);
		worker = get_worker_for_node(ref->node_id);
		j_cmd = Common::CMD_WORKER_DELIVER_RASTER;
		w_req.reset(new RasterDeliveryRequest(req.semantic_id, req.query, ref->cache_id, req.query_mode));
	}
	// Puzzle -- only if we cover more than 10%
	else if (res.has_hit() && res.coverage > 0.1) {
		Log::debug("Creating raster-puzzle job, coverage: %f", res.coverage);
		std::vector<CacheRef> entries;

		for (auto id : res.ids) {
			auto ref = raster_cache.get(req.semantic_id, id);
			auto e_node = nodes.at(ref->node_id);
			// TODO: UGLY
			if (worker == 0) {
				try {
					worker = get_worker_for_node(ref->node_id);
				} catch ( NoSuchElementException &nse ) {
					// Quiet
				}
			}
			entries.push_back(CacheRef(e_node->host, e_node->port, ref->cache_id));
		}

		if ( worker == 0 )
			throw NoSuchElementException("No worker available");

		j_cmd = Common::CMD_WORKER_PUZZLE_RASTER;
		w_req.reset( new RasterPuzzleRequest(req.semantic_id, req.query,
			res.covered, res.remainder, entries, req.query_mode));
	}
	// Full miss
	else {
		Log::debug("Creating raster-create job.");
		worker = pick_worker();
		j_cmd = Common::CMD_WORKER_CREATE_RASTER;
		w_req.reset(new RasterBaseRequest(req));
	}
	try {
		worker_connections.at(worker)->process_request(
			con.id, j_cmd, *w_req
		);
		con.processing();
	} catch (NetworkException &ne) {
		Log::error("Could send request to worker: %s. Dropping worker connection.", ne.what());
		worker_connections.erase(worker);
	} catch (IllegalStateException &ise) {
		Log::error("Worker is in illegal state for processing request: %s. Dropping worker connections.",
			ise.what());
		worker_connections.erase(worker);
	} catch (std::out_of_range &oor) {
		Log::error("Worker-connection does not exist: %d", worker);
	}
}

uint64_t IndexServer::get_worker_for_node(uint32_t node_id) {
	for (auto &e : worker_connections) {
		if (e.second->get_state() == WorkerConnection::State::IDLE && e.second->node->id == node_id)
			return e.second->id;
	}
	throw NoSuchElementException("Currently no worker available.");
}

uint64_t IndexServer::pick_worker() {
	for (auto &e : worker_connections)
		if (e.second->get_state() == WorkerConnection::State::IDLE)
			return e.second->id;
	throw NoSuchElementException("Currently no worker available.");
}
