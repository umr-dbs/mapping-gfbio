/*
 * server.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef SERVER_H_
#define SERVER_H_

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

//
// Main class of the Node-Cache-Server.
// Establishes and handles:
// - control connection to the index-server
// - worker-threads
// -- a worker thread registers itself at the index-server and
// -- blocks until a command is received
//
class NodeServer {
	friend class std::thread;
public:
	NodeServer(std::string my_host, uint32_t my_port, std::string index_host,
			uint32_t index_port, int num_threads);
	virtual ~NodeServer();

	// Fires up the node-server and will return after
	// stop() is invoked by another thread
	void run();
	// Fires up the node-server in a separate thread
	// and returns it.
	std::unique_ptr<std::thread> run_async();
	// Triggers the shutdown of the node-server
	// Subsequent calls to run or run_async have undefined
	// behaviour
	virtual void stop();
protected:
	// The method invoked by all worker-threads.
	// Registers itself at the index and waits for
	// commands to process. On connection errors
	// it attempts to reconnect -- only if the
	// control-connection is alive
	void worker_loop();

	// The thread of the delivery-manager
	std::unique_ptr<std::thread> delivery_thread;

	// The currently running workers
	std::vector<std::unique_ptr<std::thread>> workers;
private:
	// Sets up the control-connection to the server
	void setup_control_connection();

	// Separation of command-processing for the workers
	void process_worker_command(uint8_t cmd, BinaryStream &stream);

	// Handles a create-request received from the index
	void process_create_request(BinaryStream &index_stream, const BaseRequest &request );

	// Handles a puzzle-request received from the index
	void process_puzzle_request(BinaryStream &index_stream, const PuzzleRequest &request );

	// Handles a delivery-request received from the index
	void process_delivery_request(BinaryStream &index_stream, const DeliveryRequest &request );

	// Process a command received on the control-connection
	void process_control_command(uint8_t cmd, BinaryStream &stream);

	// Manages the migration of the given item to this node
	void handle_reorg_move_item( const ReorgMoveItem &item, BinaryStream &index_stream );

	// Removes the given item from the local cache of this node
	void handle_reorg_remove_item( const TypedNodeCacheKey &item );


	template <typename T>
	void finish_request( BinaryStream &index_stream, const std::shared_ptr<const T> &item );


	// Confirms the movement of the given item to the index as well
	// as to the Node it was requested from
	void confirm_move( const ReorgMoveItem& item, uint64_t new_id, BinaryStream &index_stream, BinaryStream &del_stream );

	// Indicator telling if the server should shutdown
	bool shutdown;
	// Indicator to tell the workers if they are supposed to run
	// Disabled if control-connection is lost
	bool workers_up;
	// This node's id -- provided by the index-server
	uint32_t my_id;
	// This node's host-name
	std::string my_host;
	// This node's listen-port (for delivery-connections)
	uint32_t my_port;
	// The hostname of the index-server
	std::string index_host;
	// The port on the index-server
	uint32_t index_port;
	// The number of worker-threads to use
	int num_treads;
	// The control-connection
	std::unique_ptr<UnixSocket> control_connection;
	// The delivery-manager
	DeliveryManager delivery_manager;
};

#endif /* SERVER_H_ */
