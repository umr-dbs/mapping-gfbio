/*
 * indexserver.h
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#ifndef INDEX_INDEXSERVER_H_
#define INDEX_INDEXSERVER_H_

#include "cache/common.h"
#include "util/log.h"

#include <string>
#include <thread>
#include <map>
#include <vector>
#include <deque>

class Job;
class Node;
class IndexServer;

//
// Describes a currently processed job.
// Basically holds the to participating connections
// and a reference to the node.
// When a job is created, the request is completely
// read from the frontend and passed to the worker.
// We only read responses from the worker within
// a job.
//
class Job {
	typedef std::unique_ptr<SocketConnection> CP;
	typedef std::shared_ptr<Node> NP;
public:
	Job( NP &node, CP &frontend_connection, CP &worker_connection );
	Job( const Job& ) = delete;
	Job operator=( const Job& ) = delete;
	// The node, the worker-connection belongs to
	NP node;
	// The connection to deliver results to
	CP frontend_connection;
	// The used worker connection
	CP worker_connection;
};

//
// Definition of a scheduled job.
// Implementations must provide a method
// to transform the definition into a job
// Means: Send a request to the worker
//

class JobDefinition {
	typedef std::unique_ptr<SocketConnection> CP;
	typedef std::shared_ptr<Node> NP;
public:
	JobDefinition(CP &frontend_connection, std::unique_ptr<CacheRequest> &request );
	std::unique_ptr<Job> create_job( NP &node, CP worker_connection );
	// The connection which issued this job
	CP frontend_connection;
	// The unerlying request
	std::unique_ptr<CacheRequest> request;
};


//
// Represents a cache-node.
// Each node has a control connection and
// up to n workers.
// A worker may be obtained by the get_worker method.
// The caller then owns the worker connection and is responsible
// for dropping or returning it via add_worker.
// Same is with job-definitions. They should only be enqueued here
// if there is noch worker available on this node.
// The server must take care, that enqueued jobs are processed asap.
//
//

class Node {
	typedef std::unique_ptr<SocketConnection> CP;
	typedef std::unique_ptr<JobDefinition> JP;
public:
	Node( const Node& ) = delete;
	Node operator=( const Node& ) = delete;
	Node(uint32_t id, std::string host, int port, CP &control_connection) :
			id(id), host(host), port(port), control_connection(std::move(control_connection)) {
	}
	;
	// Adds an idle worker
	void add_worker(CP &con);
	// Returns an idle worker -> Must be returned or dropped
	CP get_worker();
	// Tells wheter this node has idle workers
	bool has_worker();

	// Adds a job to schedule (only to be called if there is no worker available)
	void add_pending_job(JP &jd);
	// Retrieves a pending job from the queue
	JP get_pending_job();
	// Tells wheter this node has pending jobs
	bool has_pending_job();

	// Adds the FDs of currently idle workers to the given fd_set
	// and returns the max-fd among them
	int add_idle_workers( fd_set *readfds );

	// Checks the fds of all idle workers for available data.
	// Since this should not happen, all workers with data are dropped
	void check_idle_workers( fd_set *readfds );

	// The unique id of this node
	const uint32_t id;
	// The hostname of this node
	const std::string host;
	// The port for delivery connections on this node
	const uint32_t port;
	// The control-connection
	CP control_connection;
private:
	// All idle workers
	std::deque<CP> workers;
	// All jobs to be scheduledd
	std::deque<JP> pending_jobs;
};

//
// The heart of the cache. The index
// accepts connections from the client as well
// as from cache-nodes.
// Cache-nodes have to establish a so called control-connection
// on which they send their hostname and delivery port. After
// doing so, a unique id is assigned to the node. All workers of
// this node must use this id to register themselves at the index.
//
// Client-connections may issue requests to the server.
// The server handles everything in a single thread.
//


class IndexServer {
	typedef std::unique_ptr<SocketConnection> CP;
	typedef std::shared_ptr<Node> NP;
public:
	IndexServer( int frontend_port, int node_port );
	virtual ~IndexServer();
	// Fires up the index-server and will return after
	// stop() is invoked by another thread
	void run();
	// Fires up the index-server in a separate thread
	// and returns it.
	std::unique_ptr<std::thread> run_async();
	// Triggers the shutdown of the index-server
	// Subsequent calls to run or run_async have undefined
	// behaviour
	virtual void stop();

private:
	// Checks if there are new connections from the frontend
	void check_frontend_handshake( fd_set *readfds, int frontend_socket );

	// Checks idle frontend-connections for data
	// and creates the corresponding jobs on requests
	void check_frontend_connections( fd_set *readfds );

	// Checks if there are new connections from node-servers.
	// Also handles hello requests from nodes and workers.
	void check_node_handshake( fd_set *readfds, int node_socket );

	// Checks a node's control-connection as well
	// as its idle worker-connections for errors resp. commands.
	void check_node_connections( fd_set *readfds );

	// Processes the currently running jobs
	void handle_jobs( fd_set *readfds );

	// Processes a hello from a cache-node
	// and registers the node
	void handleNodeHello( CP &connection );

	// Processes a hello from a node's worker-thread
	// and attaches the resulting connection to the
	// corresponding node-instance
	void handleWorkerRegistration( CP &connection );

	// BIG TODO
	// Fetches the right node for handling the given job
	// Desired behaviour:
	// If response is cached: Choose node which holds it
	// else: Round-robbin or sth.
	NP get_node_for_job(const std::unique_ptr<CacheRequest> &request);

	// Sends an error to the given connection, including error-code
	void send_error( const SocketConnection &con, std::string msg );

	// Indicator telling if the server should shutdown
	bool shutdown;
	// The port to accept client connections on
	int frontend_port;
	// The port to accept connections from cache-nodes on
	int node_port;
	// The currently known nodes
	std::map<uint32_t,NP> nodes;
	// The currently idle frontend-connections
	std::vector<CP> frontend_connections;
	// The currently scheduled jobs
	std::vector<std::unique_ptr<Job>> jobs;
	// Holds fds of newly accepted connections from workers
	std::vector<int> new_node_fds;
	// The next id to assign to a node
	uint32_t next_node_id;

};

#endif /* INDEX_INDEXSERVER_H_ */
