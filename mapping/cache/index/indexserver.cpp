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

Node::Node(uint32_t id, const std::string &host, uint32_t port, const Capacity &cap) :
	id(id), host(host), port(port), last_stat_update(time(nullptr)), control_connection(0), capacity(cap) {
}

void Node::update_stats(const NodeStats& stats) {
	capacity = stats;
	query_stats += stats.query_stats;
}

const Capacity& Node::get_capacity() const {
	return capacity;
}

const QueryStats& Node::get_query_stats() const {
	return query_stats;
}

void Node::reset_query_stats() {
	query_stats.reset();
}

std::string Node::to_string() const {
	std::ostringstream ss;
	ss << "Node " << id << "[" << std::endl;
	ss << "  " << capacity.to_string() << std::endl;
	ss << "  " << query_stats.to_string() << std::endl;
	ss << "]";
	return ss.str();
}

////////////////////////////////////////////////////////////
//
// INDEX SERVER
//
////////////////////////////////////////////////////////////

IndexServer::IndexServer(int port, time_t update_interval, const std::string &reorg_strategy, const std::string &relevance_function) :
	caches(reorg_strategy,relevance_function), port(port), shutdown(false), next_node_id(1),
	query_manager(caches,nodes), last_reorg(time(nullptr)), update_interval(update_interval) {
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

	std::vector<NewConnection> new_cons;

	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds, writefds;
		FD_ZERO(&readfds);
		FD_ZERO(&writefds);

		// Add listen socket
		FD_SET(listen_socket, &readfds);

		int maxfd = listen_socket;

		// Add newly accepted sockets
		for (auto &nc : new_cons) {
			FD_SET(nc.fd, &readfds);
			maxfd = std::max(maxfd, nc.fd);
		}

		// Setup existing connections
		maxfd = std::max(maxfd, setup_fdset(&readfds, &writefds));

		int sel_ret = select(maxfd + 1, &readfds, &writefds, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		}
		else if (sel_ret > 0) {
			process_worker_connections(&readfds,&writefds);
			process_control_connections(&readfds, &writefds);
			process_client_connections(&readfds, &writefds);

			process_handshake(new_cons, &readfds);

			// Accept new connections
			if (FD_ISSET(listen_socket, &readfds)) {
				struct sockaddr_storage remote_addr;
				socklen_t sin_size = sizeof(remote_addr);
				int new_fd = accept(listen_socket, (struct sockaddr *) &remote_addr, &sin_size);
				if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
					Log::error("Accept failed: %d", strerror(errno));
				}
				else if (new_fd > 0) {
					char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];

					getnameinfo ((struct sockaddr *) &remote_addr, sin_size,
					             hbuf, sizeof hbuf,
					             sbuf, sizeof sbuf,
					             NI_NUMERICHOST | NI_NUMERICSERV);

					Log::debug("New connection established host: %s, service: %s, fd: %d", hbuf, sbuf, new_fd);
					new_cons.push_back( NewConnection(hbuf,new_fd) );
				}
			}
		}
		// Schedule Jobs
		query_manager.schedule_pending_jobs(worker_connections);

		if ( update_interval == 0 )
			continue;

		// Update stats
		time_t now = time(nullptr);
		time_t oldest_stats = now;
		bool all_idle = true;
		for (auto &kv : nodes) {
			Node& node = *kv.second;
			oldest_stats = std::min(oldest_stats, node.last_stat_update);
			ControlConnection &cc = *control_connections.at(node.control_connection);
			// Fetch stats
			if (cc.get_state() == ControlState::IDLE && (now - node.last_stat_update) > update_interval) {
				cc.send_get_stats();
			}
			// Remeber if all connections are idle -> Allows reorg
			all_idle &= (cc.get_state() == ControlState::IDLE);
		}

		// Trace stats
//		if ( all_idle && now -oldest_stats >= 10 ) {
//			fprintf(stderr, "%s\n", stats_string().c_str() );
//		}

		// Reorganize
		if (oldest_stats > last_reorg && all_idle && caches.require_reorg(nodes) ) {
			reorganize();
		}
	}

	close(listen_socket);
	Log::info("Index-Server done.");
}

int IndexServer::setup_fdset(fd_set* readfds, fd_set *writefds) {
	int maxfd = -1;

	auto wit = worker_connections.begin();
	while (wit != worker_connections.end()) {
		WorkerConnection &wc = *wit->second;
		if (wc.is_faulty()) {
			query_manager.worker_failed(wc.id);
			worker_connections.erase(wit++);
		}
		else if (wc.is_writing()) {
			FD_SET(wc.get_write_fd(), writefds);
			maxfd = std::max(maxfd, wc.get_write_fd());
			wit++;
		}
		else {
			FD_SET(wc.get_read_fd(), readfds);
			maxfd = std::max(maxfd, wc.get_read_fd());
			wit++;
		}
	}

	auto ccit = control_connections.begin();
	while (ccit != control_connections.end()) {
		ControlConnection &cc = *ccit->second;
		if (cc.is_faulty()) {
			caches.remove_all_by_node(cc.node->id);
			nodes.erase(cc.node->id);
			query_manager.node_failed(cc.node->id);
			control_connections.erase(ccit++);
		}
		else if (cc.is_writing()) {
			FD_SET(cc.get_write_fd(), writefds);
			maxfd = std::max(maxfd, cc.get_write_fd());
			ccit++;
		}
		else {
			FD_SET(cc.get_read_fd(), readfds);
			maxfd = std::max(maxfd, cc.get_read_fd());
			ccit++;
		}
	}

	auto clit = client_connections.begin();
	while (clit != client_connections.end()) {
		ClientConnection &cc = *clit->second;
		if (cc.is_faulty()) {
			if ( cc.get_state() != ClientState::IDLE ) {
				Log::info("Client connection cancelled: %ld", cc.id);
				query_manager.handle_client_abort(cc.id);
			}
			client_connections.erase(clit++);
		}
		else if (cc.is_writing()) {
			FD_SET(cc.get_write_fd(), writefds);
			maxfd = std::max(maxfd, cc.get_write_fd());
			clit++;
		}
		else {
			FD_SET(cc.get_read_fd(), readfds);
			maxfd = std::max(maxfd, cc.get_read_fd());
			clit++;
		}
	}
	return maxfd;
}

void IndexServer::process_handshake(std::vector<NewConnection> &new_fds, fd_set* readfds) {
	auto it = new_fds.begin();
	while (it != new_fds.end()) {
		if (FD_ISSET(it->fd, readfds)) {
			try {
				std::unique_ptr<BinaryFDStream> us = make_unique<BinaryFDStream>(it->fd, it->fd,true);
				BinaryStream &s = *us;

				uint32_t magic;
				s.read(&magic);
				switch (magic) {
					case ClientConnection::MAGIC_NUMBER: {
						std::unique_ptr<ClientConnection> cc = make_unique<ClientConnection>(std::move(us));
						Log::trace("New client connections established");
						client_connections.emplace(cc->id, std::move(cc));
						break;
					}
					case WorkerConnection::MAGIC_NUMBER: {
						uint32_t node_id;
						s.read(&node_id);
						std::unique_ptr<WorkerConnection> wc = make_unique<WorkerConnection>(std::move(us),
							nodes.at(node_id));
						Log::info("New worker registered for node: %d, id: %d", node_id, wc->id);
						worker_connections.emplace(wc->id, std::move(wc));
						break;
					}
					case ControlConnection::MAGIC_NUMBER: {
						std::unique_ptr<ControlConnection> cc = make_unique<ControlConnection>(std::move(us),it->hostname);
						control_connections.emplace(cc->id, std::move(cc));
						break;
					}
					default:
						Log::warn("Received unknown magic-number: %d. Dropping connection.", magic);
				}
			} catch (const std::exception &e) {
				Log::error("Error on new connection: %s. Dropping.", e.what());
			}
			it = new_fds.erase(it);
		}
		else {
			++it;
		}
	}
}

void IndexServer::process_control_connections(fd_set* readfds, fd_set* writefds) {
	for (auto &e : control_connections) {
		ControlConnection &cc = *e.second;
		// Check if node is waiting for a confirmation
		if ( cc.get_state() == ControlState::MOVE_RESULT_READ ) {
			auto res = cc.get_move_result();
			IndexCacheKey old(res.from_node_id, res.semantic_id, res.entry_id);
			if ( !query_manager.is_locked(res.type, old) )
				cc.confirm_move();
		}
		else if ( cc.get_state() == ControlState::REMOVE_REQUEST_READ ) {
			auto &node_key = cc.get_remove_request();
			IndexCacheKey key(cc.node->id, node_key.semantic_id, node_key.entry_id );
			if ( !query_manager.is_locked( node_key.type, key ) )
				cc.confirm_remove();
		}
		// Normal handling
		else if (cc.is_writing() && FD_ISSET(cc.get_write_fd(), writefds)) {
			cc.output();
		}
		else if (!cc.is_writing() && FD_ISSET(cc.get_read_fd(), readfds)) {
			// Read from connection
			cc.input();
			// Skip faulty connections
			if (cc.is_faulty() || cc.is_reading())
				continue;

			switch (cc.get_state()) {
				case ControlState::HANDSHAKE_READ: {
					auto &hs = cc.get_handshake();
					std::shared_ptr<Node> node = make_unique<Node>(next_node_id++, cc.hostname, hs.port, hs);
					node->control_connection = cc.id;
					nodes.emplace(node->id, node);
					Log::info("New node registered. ID: %d, hostname: %s, control-connection-id: %d", node->id, cc.hostname.c_str(), cc.id);
					caches.process_handshake(node->id,hs);
					cc.confirm_handshake(node);
					break;
				}
				case ControlState::MOVE_RESULT_READ: {
					Log::trace("Node %d migrated one cache-entry.", cc.node->id);
					auto res = cc.get_move_result();
					handle_reorg_result(res);
					IndexCacheKey old(res.from_node_id, res.semantic_id, res.entry_id);
					if ( !query_manager.is_locked(res.type, old) )
						cc.confirm_move();
					break;
				}
				case ControlState::REMOVE_REQUEST_READ: {
					Log::trace("Node %d requested removal of entry: %s", cc.node->id, cc.get_remove_request().to_string().c_str() );
					auto &node_key = cc.get_remove_request();
					IndexCacheKey key(cc.node->id, node_key.semantic_id, node_key.entry_id );
					if ( !query_manager.is_locked( node_key.type, key ) )
						cc.confirm_remove();
					break;
				}
				case ControlState::REORG_FINISHED:
					Log::debug("Node %d finished reorganization.", cc.node->id);
					cc.release();
					break;
				case ControlState::STATS_RECEIVED: {
					auto &stats = cc.get_stats();
					Log::debug("Node %d delivered fresh statistics: %s", cc.node->id, stats.to_string().c_str());
					cc.node->update_stats(stats);
					time(&cc.node->last_stat_update);
					caches.update_stats(cc.node->id, stats);
					cc.release();
					break;
				}
				default:
					throw IllegalStateException(
						concat("Illegal control-connection state after read: ", (int) cc.get_state()));
			}
		}
	}
}

void IndexServer::handle_reorg_result(const ReorgMoveResult& res) {
	IndexCacheKey old(res.from_node_id, res.semantic_id, res.entry_id);
	IndexCacheKey new_key(res.to_node_id, res.semantic_id, res.to_cache_id);
	caches.get_cache(res.type).move(old,new_key);
}

void IndexServer::process_client_connections(fd_set* readfds, fd_set* writefds) {
	for (auto &e : client_connections) {
		ClientConnection &cc = *e.second;
		if (cc.is_writing() && FD_ISSET(cc.get_write_fd(), writefds)) {
			cc.output();
		}
		else if (!cc.is_writing() && FD_ISSET(cc.get_read_fd(), readfds)) {
			// Read from connection
			cc.input();

			// Skip faulty connections
			if (cc.is_faulty() || cc.is_reading())
				continue;
			// Handle state-changes
			switch (cc.get_state()) {
				case ClientState::AWAIT_RESPONSE:
					Log::info("Client-request read: %s", cc.get_request().to_string().c_str() );

					query_manager.add_request(cc.id, cc.get_request());
					break;
				default:
					throw IllegalStateException(
						concat("Illegal client-connection state after read: ", (int) cc.get_state()));
			}
		}
	}
}

void IndexServer::process_worker_connections(fd_set* readfds, fd_set* writefds) {
	for (auto &e : worker_connections) {
		WorkerConnection &wc = *e.second;
		if (wc.is_writing() && FD_ISSET(wc.get_write_fd(), writefds)) {
			wc.output();
		}
		else if (!wc.is_writing() && FD_ISSET(wc.get_read_fd(), readfds)) {
			// Read from connection
			wc.input();

			// Skip faulty connections
			if (wc.is_faulty() || wc.is_reading())
				continue;
			// Handle state-changes
			switch (wc.get_state()) {
				case WorkerState::ERROR: {
					Log::warn("Worker returned error: %s. Forwarding to client.",
						wc.get_error_message().c_str());
					query_manager.close_worker(wc.id);
					auto clients = query_manager.release_worker(wc.id);
					for (auto &cid : clients) {
						try {
							client_connections.at(cid)->send_error(wc.get_error_message());
						} catch (const std::out_of_range &oor) {
							Log::warn("Client %d does not exist.", cid);
						}
					}
					wc.release();
					break;
				}
				case WorkerState::DONE: {
					Log::debug("Worker returned result. Determinig delivery qty.");
					size_t qty = query_manager.close_worker(wc.id);
					wc.send_delivery_qty(qty);
					break;
				}
				case WorkerState::DELIVERY_READY: {
					Log::debug("Worker returned delivery: %s", wc.get_result().to_string().c_str());
					auto clients = query_manager.release_worker(wc.id);
					for (auto &cid : clients) {
						try {
							client_connections.at(cid)->send_response(wc.get_result());
						} catch (const std::out_of_range &oor) {
							Log::warn("Client %d does not exist.", cid);
						}
					}
					wc.release();
					break;
				}
				case WorkerState::NEW_ENTRY: {
					Log::debug("Worker added new raster-entry");
					std::shared_ptr<IndexCacheEntry> entry( new IndexCacheEntry(wc.node->id, wc.get_new_entry()) );
					caches.get_cache( wc.get_new_entry().type ).put(entry);
					wc.entry_cached();
					break;
				}
				case WorkerState::QUERY_REQUESTED: {
					Log::debug("Worker issued cache-query: %s", wc.get_query().to_string().c_str());
					query_manager.process_worker_query(wc);
					break;
				}
				default: {
					throw IllegalStateException(
						concat("Illegal worker-connection state after read: ", (int) wc.get_state()));
				}
			}
		}
	}
}

void IndexServer::reorganize(bool force) {
	std::map<uint32_t, NodeReorgDescription> reorgs;
	for (auto &kv : nodes)
		reorgs.emplace(kv.first, NodeReorgDescription(kv.second));

	// Remember time of this reorg
	time(&last_reorg);
	caches.reorganize(nodes,reorgs,force);
	for (auto &d : reorgs) {
		Log::debug("Processing removals locally and sending reorg-commands to nodes.");
		for (auto &rm : d.second.get_removals()) {
			caches.get_cache(rm.type).remove(IndexCacheKey(d.first, rm.semantic_id, rm.entry_id));
		}

		auto &cc = control_connections.at(d.second.node->control_connection);
		if (!d.second.is_empty())
			cc->send_reorg(d.second);
	}
}

std::string IndexServer::stats_string() const {
	std::ostringstream out;
	out << "============ STATISTICS ============" << std::endl;
	out << query_manager.get_stats().to_string() << std::endl;
	for ( auto &p : nodes )
		out << p.second->to_string() << std::endl;
	out << "====================================" << std::endl;
	return out.str();
}
