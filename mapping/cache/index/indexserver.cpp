/*
 * indexserver.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "cache/index/reorg_strategy.h"
#include "util/make_unique.h"
#include "util/concat.h"

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

////////////////////////////////////////////////////////////
//
// NODE
//
////////////////////////////////////////////////////////////

Node::Node(uint32_t id, const std::string &host, uint32_t port, const Capacity &capacity) :
	id(id), host(host), port(port), capacity(capacity), last_stat_update(time(nullptr)), control_connection(-1) {
}

////////////////////////////////////////////////////////////
//
// INDEX SERVER
//
////////////////////////////////////////////////////////////

IndexServer::IndexServer(int port, ReorgStrategy &reorg_strategy) :
	raster_cache(reorg_strategy), port(port), shutdown(false), next_node_id(1),
	query_manager(raster_cache, nodes), last_reorg(time(nullptr)) {
}

IndexServer::~IndexServer() {
}

void IndexServer::stop() {
	Log::info("Shutting down.");
	shutdown = true;
}

void IndexServer::run() {
	int listen_socket = CacheCommon::get_listening_socket(port);
	Log::info("index-server: listening on node-port: %d", port);

	std::vector<int> new_fds;

	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		// Add listen sockets
		FD_SET(listen_socket, &readfds);

		int maxfd = listen_socket;

		for (auto &fd : new_fds) {
			FD_SET(fd, &readfds);
			maxfd = std::max(maxfd, fd);
		}

		maxfd = std::max(maxfd, setup_fdset(&readfds));

		int sel_ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		}
		else if (sel_ret > 0) {
			process_worker_connections(&readfds);
			process_control_connections(&readfds);
			process_client_connections(&readfds);

			process_handshake(new_fds, &readfds);

			// Accept new connections
			if (FD_ISSET(listen_socket, &readfds)) {
				struct sockaddr_storage remote_addr;
				socklen_t sin_size = sizeof(remote_addr);
				int new_fd = accept(listen_socket, (struct sockaddr *) &remote_addr, &sin_size);
				if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
					Log::error("Accept failed: %d", strerror(errno));
				}
				else if (new_fd > 0) {
					Log::debug("New connection established on fd: %d", new_fd);
					new_fds.push_back(new_fd);
				}
			}
		}
		// Schedule Jobs
		query_manager.schedule_pending_jobs(worker_connections);



		time_t now = time(nullptr);
		time_t oldest_stats = now;
		bool all_idle = true;
		for ( auto &kv : nodes ) {
			Node& node = *kv.second;
			oldest_stats = std::min(oldest_stats,node.last_stat_update);
			ControlConnection &cc = *control_connections.at(node.control_connection);
			// Fetch stats
			if ( cc.get_state() == ControlConnection::State::IDLE &&
				 (now - node.last_stat_update) > 10 ) {
				cc.send_get_stats();
			}
			// Remeber if all connections are idle -> Allows reorg
			all_idle &= ( cc.get_state() == ControlConnection::State::IDLE );
		}

		// Reorganize
		if ( oldest_stats > last_reorg && all_idle && raster_cache.requires_reorg( nodes) ) {
			// Remember time of this reorg
			time(&last_reorg);
			for ( auto &d : raster_cache.reorganize(nodes) ) {
				auto &cc = control_connections.at( nodes.at(d.node_id)->control_connection );
				cc->send_reorg( d );
			}
		}
	}
	close(listen_socket);
	Log::info("Index-Server done.");
}

int IndexServer::setup_fdset(fd_set* readfds) {
	int maxfd = -1;

	{
		auto wit = worker_connections.begin();
		while (wit != worker_connections.end()) {
			WorkerConnection &wc = *wit->second;
			if (wc.is_faulty()) {
				// TODO: Reschedule
				worker_connections.erase(wit++);
			}
			else {
				FD_SET(wc.get_read_fd(), readfds);
				maxfd = std::max(maxfd, wc.get_read_fd());
				wit++;
			}
		}
	}

	{
		auto ccit = control_connections.begin();
		while (ccit != control_connections.end()) {
			ControlConnection &cc = *ccit->second;
			if (cc.is_faulty()) {
				raster_cache.remove_all_by_node(cc.node->id);
				nodes.erase(cc.node->id);
				control_connections.erase(ccit++);
			}
			else {
				FD_SET(cc.get_read_fd(), readfds);
				maxfd = std::max(maxfd, cc.get_read_fd());
				ccit++;
			}
		}
	}

	{
		auto clit = client_connections.begin();
		while (clit != client_connections.end()) {
			ClientConnection &cc = *clit->second;
			if (cc.is_faulty()) {
				client_connections.erase(clit++);
			}
			else {
				FD_SET(cc.get_read_fd(), readfds);
				maxfd = std::max(maxfd, cc.get_read_fd());
				clit++;
			}
		}
	}

	return maxfd;
}

void IndexServer::process_handshake(std::vector<int> &new_fds, fd_set* readfds) {
	auto it = new_fds.begin();
	while (it != new_fds.end()) {
		if (FD_ISSET(*it, readfds)) {
			try {
				std::unique_ptr<UnixSocket> us = make_unique<UnixSocket>(*it, *it);
				BinaryStream &s = *us;

				uint32_t magic;
				s.read(&magic);
				switch (magic) {
					case ClientConnection::MAGIC_NUMBER: {
						std::unique_ptr<ClientConnection> cc = make_unique<ClientConnection>(std::move(us));
						Log::debug("New client connections established");
						client_connections.emplace(cc->id, std::move(cc));
						break;
					}
					case WorkerConnection::MAGIC_NUMBER: {
						uint32_t node_id;
						s.read(&node_id);
						Log::info("New worker registered for node: %d", node_id);
						std::unique_ptr<WorkerConnection> wc = make_unique<WorkerConnection>(std::move(us),
							nodes.at(node_id));
						worker_connections.emplace(wc->id, std::move(wc));
						break;
					}
					case ControlConnection::MAGIC_NUMBER: {
						NodeHandshake hs(s);
						std::shared_ptr<Node> node = make_unique<Node>(next_node_id++, hs.host, hs.port, hs);
						std::unique_ptr<ControlConnection> cc = make_unique<ControlConnection>(std::move(us), node);
						node->control_connection = cc->id;
						Log::info("New node registered. ID: %d, control-connected fd: %d", node->id,
							cc->get_read_fd());
						control_connections.emplace(cc->id, std::move(cc));
						nodes.emplace(node->id, node);

						for ( auto &raster : hs.get_raster_entries() )
							raster_cache.put( IndexCacheEntry(node->id, raster) );
						break;
					}
					default: {
						Log::warn("Received unknown magic-number: %d. Dropping connection.", magic);
					}
				}
			} catch (std::exception &e) {
				Log::error("Error on new connection: %s. Dropping.", e.what());
			}
			it = new_fds.erase(it);
		}
		else {
			++it;
		}
	}
}

void IndexServer::process_control_connections(fd_set* readfds) {
	for (auto &e : control_connections) {
		if (FD_ISSET(e.second->get_read_fd(), readfds)) {
			// Read from connection
			ControlConnection &cc = *e.second;
			cc.input();
			// Skip faulty connections
			if (cc.is_faulty())
				continue;

			switch (cc.get_state()) {
				case ControlConnection::State::REORG_RESULT_READ: {
					Log::trace("Node %d migrated one cache-entry.", cc.node->id);
					auto &res = cc.get_result();
					handle_reorg_result(res);
					cc.confirm_reorg();
					break;
				}
				case ControlConnection::State::REORG_FINISHED: {
					Log::debug("Node %d finished reorganization.", cc.node->id);
					cc.release();
					break;
				}
				case ControlConnection::State::STATS_RECEIVED: {
					Log::debug("Node %d delivered fresh statistics.", cc.node->id);
					auto &stats = cc.get_stats();
					cc.node->capacity = stats;
					time(&cc.node->last_stat_update);
					raster_cache.update_stats(cc.node->id,stats.raster_stats);
					cc.release();
					break;
				}
				default: {
					throw std::runtime_error("Unknown state of control-connection.");
				}
			}



		}
	}
}


void IndexServer::handle_reorg_result(const ReorgResult& res) {
	switch ( res.type ) {
		case ReorgResult::Type::RASTER: {
			IndexCacheKey old( res.from_node_id, res.semantic_id, res.from_cache_id );
			IndexCacheKey new_key( res.to_node_id, res.semantic_id, res.to_cache_id );
			raster_cache.move(old,new_key);
			break;
		}
		default: {
			throw ArgumentException(concat("Type: ", (int) res.type, " not supported yet."));
		}
	}
}

void IndexServer::process_client_connections(fd_set* readfds) {
	for (auto &e : client_connections) {
		if (FD_ISSET(e.second->get_read_fd(), readfds)) {
			// Read from connection
			ClientConnection &cc = *e.second;
			cc.input();

			// Skip faulty connections
			if (cc.is_faulty())
				continue;
			// Handle state-changes
			switch (cc.get_state()) {
				case ClientConnection::State::AWAIT_RESPONSE: {
					process_client_request(cc);
					break;
				}
				case ClientConnection::State::IDLE: {
					Log::error("Client-connection MUST not be in idle-state after reading from it");
				}
				default: {
					throw std::runtime_error("Unknown state of client-connection.");
				}
			}
		}
	}
}

void IndexServer::process_worker_connections(fd_set* readfds) {
	for (auto &e : worker_connections) {
		if (FD_ISSET(e.second->get_read_fd(), readfds)) {
			// Read from connection
			WorkerConnection &wc = *e.second;
			wc.input();

			// Skip faulty connections
			if (wc.is_faulty())
				continue;
			// Handle state-changes
			switch (wc.get_state()) {
				case WorkerConnection::State::ERROR: {
					Log::warn("Worker returned error: %s. Forwarding to client.",
						wc.get_error_message().c_str());

					auto clients = query_manager.release_worker(wc.id);
					for ( auto &cid : clients ) {
						try {
							client_connections.at(cid)->send_error(wc.get_error_message());
						} catch ( std::out_of_range &oor ) {
							Log::warn("Client %d does not exist.", cid );
						}
					}
					wc.release();
					break;
				}
				case WorkerConnection::State::DONE: {
					Log::debug("Worker returned result. Determinig delivery qty.");
					size_t qty = query_manager.close_worker( wc.id );
					wc.send_delivery_qty(qty);
					break;
				}
				case WorkerConnection::State::DELIVERY_READY: {
					Log::debug("Worker returned delivery: %s", wc.get_result().to_string().c_str() );
					auto clients = query_manager.release_worker(wc.id);
					for ( auto &cid : clients ) {
						try {
							client_connections.at(cid)->send_response(wc.get_result());
						} catch ( std::out_of_range &oor ) {
							Log::warn("Client %d does not exist.", cid );
						}
					}
					wc.release();
					break;
				}
				case WorkerConnection::State::NEW_RASTER_ENTRY: {
					Log::debug("Worker added new raster-entry");
					auto &ref  = wc.get_new_raster_entry();
					IndexCacheEntry entry(wc.node->id, ref);
					raster_cache.put(entry);
					wc.raster_cached();
					break;
				}
				case WorkerConnection::State::RASTER_QUERY_REQUESTED: {
					Log::debug("Worker issued raster-query: %s", wc.get_raster_query().to_string().c_str());
					process_worker_raster_query(wc);
					break;
				}
				default: {
					throw std::runtime_error(
						concat("Illegal worker-connection state after read: ", (int) wc.get_state()));
				}
			}
		}
	}
}

void IndexServer::process_client_request(ClientConnection& con) {
	switch (con.get_request_type()) {
		case ClientConnection::RequestType::RASTER:
			query_manager.add_raster_request( con.id, con.get_request() );
			break;
		default:
			throw std::runtime_error("Request-type not implemented yet.");
	}
}

void IndexServer::process_worker_raster_query(WorkerConnection& con) {
	auto &req = con.get_raster_query();
	auto res = raster_cache.query(req.semantic_id, req.query);
	Log::debug("QueryResult: %s", res.to_string().c_str());

	// Full single hit
	if (res.keys.size() == 1 && !res.has_remainder()) {
		Log::debug("Full HIT. Sending reference.");
		IndexCacheKey key( req.semantic_id, res.keys.at(0) );
		auto ref = raster_cache.get(key);
		auto node = nodes.at(ref.node_id);
		CacheRef cr(node->host, node->port, ref.entry_id);
		con.send_hit(cr);
	}
	// Puzzle
	else if (res.has_hit() && res.coverage > 0.1) {
		Log::debug("Partial HIT. Sending puzzle-request, coverage: %f", res.coverage);
		std::vector<CacheRef> entries;
		for (auto id : res.keys) {
			IndexCacheKey key(req.semantic_id, id);
			auto ref = raster_cache.get(key);
			auto &node = nodes.at(ref.node_id);
			entries.push_back(CacheRef(node->host, node->port, ref.entry_id));
		}
		PuzzleRequest pr(req.semantic_id, req.query, res.covered, res.remainder, entries);
		con.send_partial_hit(pr);
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		con.send_miss();
	}
}
