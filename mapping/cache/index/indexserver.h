/*
 * indexserver.h
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#ifndef INDEX_INDEXSERVER_H_
#define INDEX_INDEXSERVER_H_

#include "cache/index/node.h"
#include "cache/index/index_cache_manager.h"
#include "cache/index/querymanager.h"

#include "cache/common.h"

#include "cache/priv/connection.h"
#include "cache/priv/shared.h"
#include "cache/priv/requests.h"
#include "cache/priv/redistribution.h"

#include "util/log.h"

#include <string>
#include <thread>
#include <map>
#include <vector>
#include <deque>


/**
 * The heart of the cache.
 * The index accepts connections from clients as well
 * as from cache-nodes.
 * Cache-nodes have to establish a so called control-connection
 * on which they send their hostname and delivery port. After
 * doing so, a unique id is assigned to the node. All workers of
 * this node must use this id to register themselves at the index.
 *
 * Client-connections may issue requests to the server.
 * The server handles everything in a single thread.
 */
class IndexServer {
	friend class TestIdxServer;
public:
	/**
	 * Constructs a new instance
	 * @param port the port to listen on
	 * @param update_interval the interval for fetching fresh statistics and triggering reorganization (in ms).
	 * @param reorg_strategy the name of the reorg-strategy to use
	 * @param relevance_function the name of the relevance-function to use
	 */
	IndexServer( int port, time_t update_interval, const std::string &reorg_strategy, const std::string &relevance_function, bool enable_batching, const std::string &scheduler = "default" );
	virtual ~IndexServer() = default;

	/* Fires up the index-server and will return after
	 * stop() is invoked by another thread
	 */
	void run();

	/* Triggers the shutdown of the index-server
	 * Subsequent calls to run or run_async have undefined
	 * behaviour
	 */
	virtual void stop();
private:
	/**
	 * Adds the fds of all connections to the read-/write-set and kills faulty connections
	 */
	void setup_fdset( struct pollfd *fds, size_t &pos );

	/**
	 * Processes the handshake with newly accepted connections
	 * @param new_fds the accepted but not initialized connections
	 * @param readfds the set of fds
	 */
	void process_handshake( std::vector<std::unique_ptr<NewNBConnection>> &new_fds );

	/**
	 * Processes actions on control-connections
	 * @param readfds the set of fds to wait for data to read
	 * @param writefds the set of fds to wait for data to write
	 */
	void process_nodes();


	/**
	 * Processes actions on control-connections
	 * @param cc the connection to handle
	 */
	void process_control_connection(Node &node);

	/**
	 * Processes actions on worker-connections
	 * @param the connection to handle
	 */
	void process_worker_connections(Node &node);

	/**
	 * Processes actions on client-connections
	 * @param readfds the set of fds to wait for data to read
	 * @param writefds the set of fds to wait for data to write
	 */
	void process_client_connections();

	// Adjusts the cache according to the given reorg
	/**
	 * After successful movement of entries, this method reflects the new location
	 * to the global cache.
	 * @param res the information about the new location of an entry
	 */
	void handle_reorg_result( const ReorgMoveResult &res );

	/**
	 * @return a humand readable statistics string
	 */
	std::string stats_string() const;

	/**
	 * Triggers reorganization if required
	 * @param force whether to force reorg
	 */
	void reorganize(bool force = false);

	void wakeup();

	typedef std::map<uint64_t,std::unique_ptr<ClientConnection>> client_map;

	client_map::iterator suspend_client( client_map::iterator element );
	client_map::iterator resume_client( client_map::iterator element );

	// The currently known nodes
	std::map<uint32_t,std::shared_ptr<Node>> nodes;
	// Connections
//	std::map<uint64_t,std::unique_ptr<ControlConnection>> control_connections;
//	std::map<uint64_t,std::unique_ptr<WorkerConnection>>  worker_connections;
	std::map<uint64_t,std::unique_ptr<ClientConnection>>  client_connections;
	std::map<uint64_t,std::unique_ptr<ClientConnection>>  suspended_client_connections;

	IndexCacheManager caches;

	// The port the index-server is listening on
	int port;

	// Indicator telling if the server should shutdown
	bool shutdown;

	// The next id to assign to a node
	uint32_t next_node_id;

	// The query-manager handling request-scheduling
	std::unique_ptr<QueryManager> query_manager;

	// timestamp of the last reorganization
	time_t last_reorg;

	// Interval for stats-updates and reorg (in ms)
	time_t update_interval;

	BinaryStream wakeup_pipe;
};

#endif /* INDEX_INDEXSERVER_H_ */
