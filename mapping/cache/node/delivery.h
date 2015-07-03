/*
 * delivery.h
 *
 *  Created on: 03.07.2015
 *      Author: mika
 */

#ifndef DELIVERY_H_
#define DELIVERY_H_

#include "cache/priv/types.h"
#include "cache/priv/connection.h"
#include "operators/operator.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
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
	friend class DeliveryConnection;

public:
	DeliveryManager(uint32_t listen_port);
	~DeliveryManager();
	//
	// Adds the given result to the delivery queue.
	// The returned id must be used by clients fetching
	// the stored result.
	//
	uint64_t add_raster_delivery(std::unique_ptr<GenericRaster> &result, unsigned int count = 1);
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
	// map and sends it. Throws std::out_of_range if no delivery is present
	// for the given id
	Delivery& get_delivery(uint64_t id);

	void remove_delivery(uint64_t id);

	void remove_expired_deliveries();

	// Indicator telling if the manager should shutdown
	bool shutdown;
	// The port the manager listens at
	uint32_t listen_port;
	// the mutex used to acces the stored deliveries
	std::mutex delivery_mutex;
	// The counter for the delivery-ids
	uint64_t delivery_id;
	// the currently stored deliveries
	std::map<uint64_t, Delivery> deliveries;
	// the currently open connections
	std::vector<std::unique_ptr<DeliveryConnection>> connections;
};


#endif /* DELIVERY_H_ */
