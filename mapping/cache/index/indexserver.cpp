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
// JOB
//
////////////////////////////////////////////////////////////

Job::Job(NP &node, CP &frontend_connection, CP &worker_connection) :
		node(node), frontend_connection(std::move(frontend_connection)), worker_connection(
				std::move(worker_connection)) {
}

////////////////////////////////////////////////////////////
//
// JOB-DEFINITION
//
////////////////////////////////////////////////////////////

JobDefinition::JobDefinition(CP &frontend_connection, uint8_t worker_cmd, std::unique_ptr<BaseRequest> &request ) :
		worker_cmd(worker_cmd), frontend_connection(std::move(frontend_connection)), request(std::move(request)) {
}

std::unique_ptr<Job> JobDefinition::create_job(NP &node, CP worker_connection) {
	// Send request to worker
	worker_connection->stream->write(worker_cmd);
	request->toStream(*worker_connection->stream);
	return std::make_unique<Job>(node, frontend_connection, worker_connection);
}

////////////////////////////////////////////////////////////
//
// NODE
//
////////////////////////////////////////////////////////////

void Node::add_worker(CP &con) {
	workers.push_back(std::move(con));
}

Node::CP Node::get_worker() {
	if (workers.empty())
		throw NoSuchElementException("No worker available");
	CP worker = std::move(workers.front());
	workers.pop_front();
	return worker;
}

bool Node::has_worker() {
	return !workers.empty();
}

void Node::add_pending_job(JP& jd) {
	pending_jobs.push_back(std::move(jd));
}

Node::JP Node::get_pending_job() {
	if (pending_jobs.empty())
		throw NoSuchElementException("No pending job available");
	JP jd = std::move(pending_jobs.front());
	pending_jobs.pop_front();
	return jd;
}

bool Node::has_pending_job() {
	return !pending_jobs.empty();
}

void Node::check_idle_workers(fd_set* readfds) {
	auto wit = workers.begin();
	while (wit != workers.end()) {
		auto &wc = *wit;
		if (FD_ISSET(wc->fd, readfds)) {
			Log::error("Idle workers should never send data. Dropping worker.");
			wit = workers.erase(wit);
		}
		else
			++wit;
	}
}

int Node::add_idle_workers(fd_set* readfds) {
	int maxfd = -1;
	for (auto &wc : workers) {
		FD_SET(wc->fd, readfds);
		maxfd = std::max(maxfd, wc->fd);
	}
	return maxfd;
}

////////////////////////////////////////////////////////////
//
// INDEX SERVER
//
////////////////////////////////////////////////////////////

IndexServer::IndexServer(int frontend_port, int node_port) :
		shutdown(false), frontend_port(frontend_port), node_port(node_port), next_node_id(1) {
}

IndexServer::~IndexServer() {
}

void IndexServer::run() {
	int node_socket = Common::get_listening_socket(node_port);
	int frontend_socket = Common::get_listening_socket(frontend_port);
	Log::info("index-server: listening on node-port: %d and frontend-port: %d", node_port, frontend_port);

	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		// Add listen sockets
		FD_SET(node_socket, &readfds);
		FD_SET(frontend_socket, &readfds);

		int maxfd = std::max(node_socket, frontend_socket);

		// Add newly accepted connections
		for (auto &fd : new_node_fds) {
			FD_SET(fd, &readfds);
			maxfd = std::max(maxfd, fd);
		}

		// Add control-connections and idle workers
		for (auto &e : nodes) {
			FD_SET(e.second->control_connection->fd, &readfds);
			maxfd = std::max(maxfd, e.second->control_connection->fd);
			maxfd = std::max(maxfd, e.second->add_idle_workers(&readfds));
		}

		// Add frontend-connections
		for (auto &fc : client_connections) {
			FD_SET(fc->fd, &readfds);
			maxfd = std::max(maxfd, fc->fd);
		}

		// Add job-connections
		for (auto &job : jobs) {
			FD_SET(job->worker_connection->fd, &readfds);
			maxfd = std::max(maxfd, job->worker_connection->fd);
		}

		int sel_ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		}
		else if (sel_ret > 0) {
			check_node_connections(&readfds);
			check_node_handshake(&readfds, node_socket);

			check_client_handshake(&readfds, frontend_socket);
			check_client_connections(&readfds);

			handle_jobs(&readfds);
		}
	}
	close(node_socket);
	close(frontend_socket);
	Log::info("Index-Server done.");
}

void IndexServer::stop() {
	Log::info("Shutting down.");
	shutdown = true;
}

std::unique_ptr<std::thread> IndexServer::run_async() {
	return std::make_unique<std::thread>(&IndexServer::run, this);
}

void IndexServer::check_node_connections(fd_set* readfds) {
	auto nit = nodes.begin();
	while (nit != nodes.end()) {
		auto node = (*nit).second;
		if (FD_ISSET(node->control_connection->fd, readfds)) {
			try {
				uint8_t cmd;
				node->control_connection->stream->read(&cmd);
				switch (cmd) {
					default: {
						std::ostringstream msg;
						msg << "Received illegal command on control-connection for node: " <<  node->id;
						throw NetworkException(msg.str());
					}
				}
				// Check idle workers
				node->check_idle_workers(readfds);
				++nit;
			} catch (NetworkException &ne) {
				Log::error("Error on control-connection for node: %d. Dropping. Reason: %s", node->id,  ne.what());
				// TODO: Redistribute queued jobs
				raster_cache.remove_all_by_node(nit->first);
				nit = nodes.erase(nit);
			}
		}
		else
			++nit;
	}
}

void IndexServer::check_node_handshake(fd_set* readfds, int node_socket) {
	// Check new node connections
	auto it = new_node_fds.begin();
	while (it != new_node_fds.end()) {
		auto &fd = *it;
		if (FD_ISSET(fd, readfds)) {
			try {
				CP con(new SocketConnection(fd));
				uint8_t cmd;
				con->stream->read(&cmd);

				switch (cmd) {
					case Common::CMD_INDEX_NODE_HELLO: {
						handleNodeHello(con);
						break;
					}
					case Common::CMD_INDEX_REGISTER_WORKER: {
						handleWorkerRegistration(con);
						break;
					}
					default: {
						std::ostringstream msg;
						msg << "Received illegal command on node-connection with fd: " <<  fd;
						throw NetworkException(msg.str());
					}
				}
			} catch ( NetworkException &ne ) {
				Log::error("Error on fresh node-connection. Dropping. Reason: %s", ne.what());
			}
			// remove from new fds
			it = new_node_fds.erase(it);
		}
		else
			++it;
	}

	// Accept new node connection
	if (FD_ISSET(node_socket, readfds)) {
		struct sockaddr_storage remote_addr;
		socklen_t sin_size = sizeof(remote_addr);
		int new_fd = accept(node_socket, (struct sockaddr *) &remote_addr, &sin_size);

		if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
			Log::error("Accept failed: %d", strerror(errno));
		}
		else if (new_fd > 0) {
			Log::debug("New node-connection established on fd: %d", new_fd);
			new_node_fds.push_back(new_fd);
		}
	}

}

void IndexServer::handleNodeHello(CP &connection) {
	std::string host;
	uint32_t port;
	try {
		connection->stream->read(&host);
		connection->stream->read(&port);

		uint32_t id = next_node_id++;

		NP node(new Node(id, host, port, connection));

		Log::info("New node registered. ID: %d, control-connected fd: %d", id, node->control_connection->fd);
		uint8_t code = Common::RESP_INDEX_NODE_HELLO;
		node->control_connection->stream->write(code);
		node->control_connection->stream->write(id);

		nodes[id] = node;
	} catch ( NetworkException &ne ) {
		Log::error("Could not register new cache-node: %s", ne.what());
	}
}

void IndexServer::handleWorkerRegistration(CP &connection) {
	uint32_t id;
	try {
		connection->stream->read(&id);
		auto n = nodes.at(id);
		Log::info("New worker-connection for Node: %d", id);
		// If we have pending jobs for this worker... assing it
		if (n->has_pending_job())
			jobs.push_back(n->get_pending_job()->create_job(n, std::move(connection)));
		// Otherwise simply add this connection
		else
			n->add_worker(connection);

	} catch (std::out_of_range &ore_) {
		Log::error("Worker connection for unknown Node: %d. Discarding.", id);
	} catch ( NetworkException &ne ) {
		Log::error("Could not register new worker: %s", ne.what());
	}
}

void IndexServer::check_client_handshake(fd_set* readfds, int frontend_socket) {
	// Accept new client connection
	if (FD_ISSET(frontend_socket, readfds)) {
		struct sockaddr_storage remote_addr;
		socklen_t sin_size = sizeof(remote_addr);
		int new_fd = accept(frontend_socket, (struct sockaddr *) &remote_addr, &sin_size);

		if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
			Log::error("Accept failed: %d", strerror(errno));
		}
		else if (new_fd > 0) {
			Log::debug("New client-connection established on fd: %d", new_fd);
			client_connections.push_back(std::make_unique<SocketConnection>(new_fd));
		}
	}
}

void IndexServer::check_client_connections(fd_set* readfds) {
	auto it = client_connections.begin();
	while (it != client_connections.end()) {
		// Remove connection from idle frontend connections?
		bool remove_connection = true;
		auto &fc = *it;
		if (FD_ISSET(fc->fd, readfds)) {
			try {
				uint8_t cmd;
				if (fc->stream->read(&cmd, true)) {
					switch (cmd) {
						case Common::CMD_INDEX_GET_RASTER: {
							RasterBaseRequest req(*fc->stream);
							try {
								Log::debug("Processing raster-request from client.");
								process_raster_request( fc, req );
							} catch (NoSuchElementException &nse) {
								Log::error("No node registered for processing this request.");
								send_error(*fc,"No worker-node available");
								remove_connection = false;
							}

							break;
						}
						default: {
							Log::warn("Unknown command on frontend-connection: %d. Dropping connection.",
									cmd);
							break;
						}
					}
				}
				else {
					Log::debug("Frontend-connection closed on fd: %d", fc->fd);
				}
			} catch (NetworkException &ne) {
				Log::error("Error on frontend-connection with fd: %d. Dropping. Reason: %s", fc->fd,
						ne.what());
			}
			if ( remove_connection )
				it = client_connections.erase(it);
			else
				++it;
		}
		else
			++it;
	}
}

void IndexServer::handle_jobs(fd_set* readfds) {
	// Holds all new jobs
	std::vector<std::unique_ptr<Job>> new_jobs;

	auto it = jobs.begin();
	while (it != jobs.end()) {
		auto &job = (*it);
		auto &wc = job->worker_connection;
		auto &fc = job->frontend_connection;
		if (FD_ISSET(wc->fd, readfds)) {
			// Job done?
			bool done = true;
			// May we reuse the connections
			bool reuse_worker = true;
			bool reuse_frontend = true;

			uint8_t resp;

			try {
				wc->stream->read(&resp);
				switch (resp) {
					case Common::RESP_WORKER_RESULT_READY: {
						// Read delivery id
						uint64_t delivery_id;
						wc->stream->read(&delivery_id);
						Log::debug("Worker returned result, delivery_id: %d", delivery_id);

						// Send delivery-id and node address
						uint8_t f_resp = Common::RESP_INDEX_GET;
						DeliveryResponse dr(job->node->host,job->node->port,delivery_id);
						fc->stream->write(f_resp);
						dr.toStream(*fc->stream);
						Log::debug("Finished processing raster-request from client.");
						break;
					}
					case Common::CMD_INDEX_QUERY_RASTER_CACHE: {
						Log::debug("Processing raster-request from worker.");
						BaseRequest req(*wc->stream);
						process_node_raster_request(wc,req);
						Log::debug("Finished processing raster-request from worker.");
						done = false;
						break;
					}
					case Common::RESP_WORKER_NEW_RASTER_CACHE_ENTRY: {
						STCacheKey key(*wc->stream);
						STRasterEntryBounds cube(*wc->stream);
						Log::debug("Worker returned new result to raster-cache, key: %s:%d", key.semantic_id.c_str(), key.entry_id);
						std::unique_ptr<STRasterRef> e = std::make_unique<STRasterRef>(job->node->id, key.entry_id, cube );
						raster_cache.put(key.semantic_id, e );
						done = false;
						break;
					}
					case Common::RESP_WORKER_ERROR: {
						std::string msg;
						wc->stream->read(&msg);
						Log::warn("Worker returned error: %s", msg.c_str());
						send_error(*fc,msg);
						break;
					}
					default: {
						Log::error("Worker returned unknown code: %d. Terminating worker-connection.", resp);
						send_error(*fc,"Internal error");
						reuse_worker = false;
						break;
					}
				}
			} catch (NetworkException &ne) {
				reuse_worker = false;
				reuse_frontend = false;
				Log::error("Error while processing job. Both connections are dropped.");
			}
			// Job done
			if (done) {
				// Release frontend connection
				if (reuse_frontend) {
					Log::debug("Releasing frontend-connection.");
					client_connections.push_back(std::move(fc));
				}

				// Schedule next if available
				if (reuse_worker && job->node->has_pending_job()) {
					Log::debug("Directly reusing worker-connection for queued job.");
					new_jobs.push_back(job->node->get_pending_job()->create_job(job->node, std::move(wc)));
				}
				else if (reuse_worker) {
					Log::debug("Releasing worker-connection.");
					job->node->add_worker(wc);
				}

				it = jobs.erase(it);
			}
			else
				++it;
		}
		// FD not set
		else
			++it;
	}
	for ( auto &j : new_jobs) {
		jobs.push_back( std::move(j) );
	}
}

void IndexServer::process_raster_request( CP &client_con, const RasterBaseRequest& req) {

	Log::debug("Querying raster-cache for: %s::%s", req.semantic_id.c_str(), Common::qr_to_string(req.query).c_str());

	STQueryResult res = raster_cache.query( req.semantic_id, req.query );

	Log::debug("QueryResult: %s", res.to_string().c_str() );

	uint8_t j_cmd;
	std::unique_ptr<BaseRequest> w_req;
	std::shared_ptr<Node> node;

	// Full single hit
	if ( res.ids.size() == 1 && !res.has_remainder() ) {
		Log::debug("Creating raster-delivery job.");
		auto ref = raster_cache.get( req.semantic_id, res.ids[0] );
		j_cmd = Common::CMD_WORKER_DELIVER_RASTER;
		node = nodes.at( ref->node_id );
		w_req.reset( new RasterDeliveryRequest(req.semantic_id,req.query,ref->cache_id,req.query_mode) );
	}
	// Puzzle -- only if we cover more than 10%
	else if ( res.has_hit() && res.coverage > 0.1 ) {
		Log::debug("Creating raster-puzzle job, coverage: %f", res.coverage);
		j_cmd = Common::CMD_WORKER_PUZZLE_RASTER;
		std::vector<CacheRef> entries;
		for ( auto id : res.ids ) {
			auto ref = raster_cache.get( req.semantic_id, id );
			auto e_node = nodes.at( ref->node_id );
			// Assing first free worker-node
			if ( node == nullptr && e_node->has_worker() )
				node = e_node;
			entries.push_back( CacheRef(e_node->host,e_node->port,ref->cache_id) );
		}
		// If no node was found...
		if ( node == nullptr )
			node = nodes.at(raster_cache.get( req.semantic_id, res.ids[0] )->node_id);

		w_req.reset( new RasterPuzzleRequest( req.semantic_id, req.query, res.covered,
				res.remainder, entries, req.query_mode ) );

	}
	// Full miss
	else {
		Log::debug("Creating raster-create job.");
		j_cmd = Common::CMD_WORKER_CREATE_RASTER;
		node = pick_worker();
		w_req.reset( new RasterBaseRequest(req) );
	}

	Log::debug("Sending request to %s:%d: %s", node->host.c_str(), node->port, w_req->to_string().c_str() );

	std::unique_ptr<JobDefinition> jd = std::make_unique<JobDefinition>(client_con,j_cmd,w_req);

	if (node->has_worker())
		jobs.push_back(jd->create_job(node, node->get_worker()));
	// Add this job to the node's queue
	else
		node->add_pending_job(jd);
}

void IndexServer::process_node_raster_request( CP &worker_con, const BaseRequest& req) {

	Log::debug("Querying raster-cache for: %s::%s", req.semantic_id.c_str(), Common::qr_to_string(req.query).c_str());

	STQueryResult res = raster_cache.query( req.semantic_id, req.query );

	Log::debug("QueryResult: %s", res.to_string().c_str() );

	uint8_t resp;

	// Full single hit
	if ( res.ids.size() == 1 && !res.has_remainder() ) {
		Log::debug("Full HIT. Sending reference.");
		auto ref = raster_cache.get( req.semantic_id, res.ids[0] );
		auto node = nodes.at( ref->node_id );
		resp = Common::RESP_INDEX_HIT;
		CacheRef cr(node->host,node->port,ref->cache_id);
		worker_con->stream->write(resp);
		cr.toStream(*worker_con->stream);
	}
	// Puzzle
	else if ( res.has_hit() && res.coverage > 0.1 ) {
		Log::debug("Partial HIT. Sending puzzle-request, coverage: %f", res.coverage);
		std::vector<CacheRef> entries;
		for ( auto id : res.ids ) {
			auto ref = raster_cache.get( req.semantic_id, id );
			auto node = nodes.at( ref->node_id );
			entries.push_back( CacheRef(node->host,node->port,ref->cache_id) );
		}
		PuzzleRequest pr( req.semantic_id, req.query, res.covered, res.remainder, entries );
		resp = Common::RESP_INDEX_PARTIAL;
		worker_con->stream->write(resp);
		pr.toStream(*worker_con->stream);
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		resp = Common::RESP_INDEX_MISS;
		worker_con->stream->write(resp);
	}
}

void IndexServer::send_error(const SocketConnection& con, std::string msg) {
	uint8_t code = Common::RESP_INDEX_ERROR;
	con.stream->write(code);
	con.stream->write(msg);
}

IndexServer::NP IndexServer::pick_worker() {
	if (nodes.empty())
		throw NoSuchElementException("No nodes available");

	// TODO: Pick node
	return nodes.begin()->second;
}

