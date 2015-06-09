/*
 * server.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef SERVER_H_
#define SERVER_H_

#include "cache/common.h"
#include "operators/operator.h"
#include "util/log.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
#include <mutex>
#include <map>

//
// Outsourced delivery-part of the node server-
// Currently single threaded. Waits for incomming
// connections and delivers the requested delivery-id
// (if valid).
//
// TODO: Add timeout to deliveries -- easy peasy
class DeliveryManager {
	friend class std::thread;
public:
	DeliveryManager(uint32_t listen_port);
	~DeliveryManager();
	//
	// Adds the given result to the delivery queue.
	// The returned id must be used by clients fetching
	// the stored result.
	//
	uint64_t add_delivery(std::unique_ptr<GenericRaster> &result);
	// Fires up the delivery-manager and will return after
	// stop() is invoked by another thread
	void run();
	// Fires up the node-server in a separate thread
	// and returns it.
	std::unique_ptr<std::thread> run_async();
	// Triggers the shutdown of the node-server
	// Subsequent calls to run or run_async have undefined
	// behaviour
	void stop();
private:
	// Fetches the delivery with the given id from the internal
	// map. Throws std::out_of_range if no delivery is present
	// for the given id
	std::unique_ptr<GenericRaster> get_delivery(uint64_t id);

	// Process a command received on a delivery-connection
	void process_delivery(uint8_t cmd, SocketConnection &con);

	// Indicator telling if the manager should shutdown
	bool shutdown;
	// The port the manager listens at
	uint32_t listen_port;
	// the mutex used to acces the stored deliveries
	std::mutex delivery_mutex;
	// The counter for the delivery-ids
	uint64_t delivery_id;
	// the currently stored deliveries
	std::map<uint64_t, std::unique_ptr<GenericRaster>> deliveries;
	// the currently open connections
	std::vector<std::unique_ptr<SocketConnection>> connections;
};

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
	void process_worker_command(uint8_t cmd, SocketConnection &con);
	// Process a command received on the control-connection
	void process_control_command(uint8_t cmd);

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
	std::unique_ptr<SocketConnection> control_connection;
	// The delivery-manager
	DeliveryManager delivery_manager;
};

#endif /* SERVER_H_ */
