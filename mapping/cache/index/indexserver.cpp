/*
 * indexserver.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/index/indexserver.h"
#include "util/make_unique.h"

#include <deque>
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

JobDefinition::JobDefinition(CP &frontend_connection, const std::string &graph_json,
		const QueryRectangle& query) :
		frontend_connection(std::move(frontend_connection)), query(query), graph_json(graph_json) {
}

// TODO: Why is there no implementation of the declared default constructor of QueryRectangle
JobDefinition::JobDefinition(CP& frontend_connection) :
		frontend_connection(std::move(frontend_connection)), query(0, 0, 0, 0, 0, 0, 0, EPSG_UNKNOWN) {
}

JobDefinition::~JobDefinition() {
}

class RasterJobDef: public JobDefinition {
public:
	RasterJobDef(const RasterJobDef&) = delete;
	RasterJobDef operator=(const RasterJobDef&) = delete;
	RasterJobDef(CP &frontend_connection);
	RasterJobDef(CP &frontend_connection, const std::string &graph_json, const QueryRectangle& query,
			GenericOperator::RasterQM query_mode);
	virtual ~RasterJobDef();
	virtual std::unique_ptr<Job> create_job(NP &node, CP worker_connection);
	GenericOperator::RasterQM query_mode;
};

RasterJobDef::RasterJobDef(CP &frontend_connection, const std::string &graph_json,
		const QueryRectangle& query, GenericOperator::RasterQM query_mode) :
		JobDefinition(frontend_connection, graph_json, query), query_mode(query_mode) {
}

RasterJobDef::RasterJobDef(CP& frontend_connection) :
		JobDefinition(frontend_connection) {
	// Read the request directly from the connection
	this->frontend_connection->stream->read(&graph_json);
	QueryRectangle q(*this->frontend_connection->stream);
	uint8_t qm;
	this->frontend_connection->stream->read(&qm);
	query = q;
	query_mode = (qm == 1) ? GenericOperator::RasterQM::EXACT : GenericOperator::RasterQM::LOOSE;
}

std::unique_ptr<Job> RasterJobDef::create_job(NP &node, CP worker_connection) {
	// Send request to worker
	uint8_t cmd = Common::CMD_WORKER_GET_RASTER;
	worker_connection->stream->write(cmd);
	Common::writeRasterRequest(*worker_connection, graph_json, query, query_mode);

	return std::make_unique<Job>(node, frontend_connection, worker_connection);
}

RasterJobDef::~RasterJobDef() {
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
			Log::error("Idle workers should never send date. Dropping worker.");
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
	int node_socket = Common::getListeningSocket(node_port);
	int frontend_socket = Common::getListeningSocket(frontend_port);
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
		for (auto &fc : frontend_connections) {
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

			check_frontend_handshake(&readfds, frontend_socket);
			check_frontend_connections(&readfds);

			handle_jobs(&readfds);
		}
	}
	close(node_socket);
	close(frontend_socket);
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

		Log::info("Connection fd: %d", connection->fd);

		NP node(new Node(id, host, port, connection));


		Log::info("New node registered. ID: %d", id);
		uint8_t code = Common::RESP_INDEX_NODE_HELLO;
		Log::info("Node-Connection fd: %d", node->control_connection->fd);
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

void IndexServer::check_frontend_handshake(fd_set* readfds, int frontend_socket) {
	// Accept new node connection
	if (FD_ISSET(frontend_socket, readfds)) {
		struct sockaddr_storage remote_addr;
		socklen_t sin_size = sizeof(remote_addr);
		int new_fd = accept(frontend_socket, (struct sockaddr *) &remote_addr, &sin_size);

		if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
			Log::error("Accept failed: %d", strerror(errno));
		}
		else if (new_fd > 0) {
			Log::debug("New front-connection established on fd: %d", new_fd);
			frontend_connections.push_back(std::make_unique<SocketConnection>(new_fd));
		}
	}
}

void IndexServer::check_frontend_connections(fd_set* readfds) {
	auto it = frontend_connections.begin();
	while (it != frontend_connections.end()) {
		// Remove connection from idle frontend connections?
		bool remove_connection = true;
		auto &fc = *it;
		if (FD_ISSET(fc->fd, readfds)) {
			try {
				uint8_t cmd;
				if (fc->stream->read(&cmd, true)) {
					switch (cmd) {
						case Common::CMD_INDEX_GET_RASTER: {
							std::unique_ptr<JobDefinition> jd = std::make_unique<RasterJobDef>(fc);
							try {
								NP node = get_node_for_job(jd);
								// We can process the job directly
								if (node->has_worker())
									jobs.push_back(jd->create_job(node, node->get_worker()));
								// Add this job to the node's queue
								else
									node->add_pending_job(jd);
							} catch (NoSuchElementException &nse) {
								// TODO: This is ugly
								fc = std::move(jd->frontend_connection);
								std::string msg = "No worker-node available";
								Log::error("No node registered for processing this request.");
								uint8_t resp = Common::RESP_INDEX_ERROR;
								fc->stream->write(resp);
								fc->stream->write(msg);
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
				it = frontend_connections.erase(it);
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
						Log::debug("Worker returned result. Delivery-ID: %d", delivery_id);

						// Send delivery-id and node address
						uint8_t f_resp = Common::RESP_INDEX_GET;
						fc->stream->write(f_resp);
						fc->stream->write(job->node->host);
						fc->stream->write(job->node->port);
						fc->stream->write(delivery_id);
						Log::debug("Worker finished processing request.");
						break;
					}
					case Common::RESP_WORKER_NEW_CACHE_ENTRY: {
						Log::info("Worker returned new result to cache.");
						done = false;
						break;
					}
					case Common::RESP_WORKER_ERROR: {
						std::string msg;
						wc->stream->read(&msg);
						Log::warn("Worker returned error %s", msg.c_str());
						uint8_t f_resp = Common::RESP_INDEX_ERROR;
						fc->stream->write(f_resp);
						fc->stream->write(msg);
						break;
					}
					default: {
						Log::error("Worker returned unknown code: %d. Terminating worker-connection.", resp);
						uint8_t f_resp = Common::RESP_INDEX_ERROR;
						std::string msg = "Internal error";
						fc->stream->write(f_resp);
						fc->stream->write(msg);
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
					frontend_connections.push_back(std::move(fc));
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

IndexServer::NP IndexServer::get_node_for_job(const std::unique_ptr<JobDefinition> &job_def) {
	// TODO: BIG TODO
	(void) job_def;
	if (nodes.empty())
		throw NoSuchElementException("No nodes available");
	else
		return nodes.begin()->second;
}
