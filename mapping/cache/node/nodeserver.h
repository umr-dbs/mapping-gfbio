/*
 * server.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef NODE_NODESERVER_H_
#define NODE_NODESERVER_H_

#include "cache/node/node_manager.h"
#include "cache/common.h"
#include "cache/node/delivery.h"
#include "cache/priv/connection.h"
#include "cache/priv/redistribution.h"
#include "operators/operator.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
#include <mutex>
#include <map>

/**
 *  Main class of the Node-Cache-Server.
 * Establishes and handles:
 * - control connection to the index-server
 * - worker-threads
 *   -- a worker thread registers itself at the index-server and
 *   -- blocks until a command is received
 */
class NodeServer {
	friend class std::thread;
	friend class TestNodeServer;
	friend class TestCacheMan;
public:

	NodeServer( std::unique_ptr<NodeCacheManager> manager, uint32_t my_port, std::string index_host,
			uint32_t index_port, int num_threads);
	~NodeServer();

	/**
	 * Fires up the node-server and will return after
	 * stop() is invoked by another thread
	 */
	void run();

	/**
	 * Fires up the node-server in a separate thread
	 * and returns it.
	 */
	std::unique_ptr<std::thread> run_async();

	/**
	 * Triggers the shutdown of the node-server
	 * Subsequent calls to run or run_async have undefined
	 * behaviour
	 */
	void stop();
private:
	/**
	 * The method invoked by all worker-threads. Registers itself at the index and waits for
	 * commands to process. On connection errors it attempts to reconnect -- only if the
	 * control-connection is alive
	 */
	void worker_loop();

	/**
	 * Sets up the control-connection to the server
	 */
	void setup_control_connection();

	/**
	 * Processes a command received from the index-server
	 * @param index_con the connection to the index-server
	 * @param payload the payload received
	 */
	void process_worker_command(BlockingConnection &index_con, BinaryReadBuffer &payload);

	/**
	 * Handles a create-request received from the index
	 * @param index_con the connection to the index-server
	 * @param request the request received
	 */
	void process_create_request(BlockingConnection &index_con, const BaseRequest &request );

	/**
	 * Handles a puzzle-request received from the index
	 * @param index_con the connection to the index-server
	 * @param request the request received
	 */
	void process_puzzle_request(BlockingConnection &index_con, const PuzzleRequest &request );

	/**
	 * Handles a delivery-request received from the index
	 * @param index_con the connection to the index-server
	 * @param request the request received
	 */
	void process_delivery_request(BlockingConnection &index_con, const DeliveryRequest &request );

	/**
	 * Finishes the processing of a request by passing the result
	 * to the delivery component and notifying the index.
	 * @param index_con the connection to the index-server
	 * @param item the computation result
	 */
	template <typename T>
	void finish_request( BlockingConnection &index_con, const std::shared_ptr<const T> &item );


	/**
	 * Process a command received on the control-connection
	 * @param payload the payload received
	 */
	void process_control_command(BinaryReadBuffer &payload);

	/**
	 * Manages the migration of the given item to this node
	 * @param item the description of the item to migrate
	 */
	void handle_reorg_move_item( const ReorgMoveItem &item );

	/**
	 * Removes the given item from the local cache of this node
	 * @param item the key of the item to remove
	 */
	void handle_reorg_remove_item( const TypedNodeCacheKey &item );


	/**
	 * Confirms the movement of the given item to the index as well
	 * as to the Node it was requested from
	 * @param del_stream the connection to the delivery component of the source-node
	 * @param item the item migrated
	 * @param new_id the new cache id of the entry
	 */
	void confirm_move( BlockingConnection &del_stream, const ReorgMoveItem& item, uint64_t new_id );

	void wakeup();

	/** Indicator telling if the server should shutdown */
	bool shutdown;

	/** Indicator to tell the workers if they are supposed to run --> Disabled if control-connection is lost */
	bool workers_up;

	/**  This node's id -- provided by the index-server */
	uint32_t my_id;

	/** This node's listen-port (for delivery-connections) */
	uint32_t my_port;

	/** This node's hostname */
	std::string my_host;

	/** The hostname of the index-server */
	std::string index_host;

	/** The port on the index-server */
	uint32_t index_port;

	/** The number of worker-threads to use */
	int num_treads;

	/** The control-connection */
	std::unique_ptr<WakeableBlockingConnection> control_connection;

	/** The delivery-manager */
	DeliveryManager delivery_manager;

	/** The thread of the delivery-manager */
	std::unique_ptr<std::thread> delivery_thread;

	/** The cache-manager */
	std::unique_ptr<NodeCacheManager> manager;

	/** The currently running worker-threads */
	std::vector<std::unique_ptr<std::thread>> workers;

	BinaryStream wakeup_pipe;
};

#endif /* NODE_NODESERVER_H_ */
