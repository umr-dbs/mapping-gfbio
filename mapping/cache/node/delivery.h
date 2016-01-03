/*
 * delivery.h
 *
 *  Created on: 03.07.2015
 *      Author: mika
 */

#ifndef DELIVERY_H_
#define DELIVERY_H_

#include "cache/node/node_manager.h"
#include "cache/priv/connection.h"
#include "operators/operator.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
#include <map>

//
// Represents to to deliver response
// Holds the item itself, a counter representing
// the number of times this may be sent as well
// as a timestamp representing its expiration-time.
//
class Delivery {
public:
	Delivery( uint64_t id, unsigned int count, std::shared_ptr<const GenericRaster> raster );
	Delivery( uint64_t id, unsigned int count, std::shared_ptr<const PointCollection> points );
	Delivery( uint64_t id, unsigned int count, std::shared_ptr<const LineCollection> lines );
	Delivery( uint64_t id, unsigned int count, std::shared_ptr<const PolygonCollection> polygons );
	Delivery( uint64_t id, unsigned int count, std::shared_ptr<const GenericPlot> plot );

	Delivery( const Delivery &d ) = delete;
	Delivery( Delivery &&d ) = default;

	Delivery& operator=(const Delivery &d) = delete;
	Delivery& operator=(Delivery &&d) = delete;

	const uint64_t id;
	const time_t creation_time;
	unsigned int count;
	void send( DeliveryConnection &connection );
private:
	CacheType type;
	std::shared_ptr<const GenericRaster> raster;
	std::shared_ptr<const PointCollection> points;
	std::shared_ptr<const LineCollection> lines;
	std::shared_ptr<const PolygonCollection> polygons;
	std::shared_ptr<const GenericPlot> plot;
};

//
// Outsourced delivery-part of the node server-
// Currently single threaded. Waits for incomming
// connections and delivers the requested delivery-id
// (if valid).
//
class DeliveryManager {
	friend class std::thread;
	friend class DeliveryConnection;

public:
	DeliveryManager(uint32_t listen_port, NodeCacheManager &manager);
	~DeliveryManager();
	//
	// Adds the given result to the delivery queue.
	// The returned id must be used by clients fetching
	// the stored result.
	//
	template <typename T>
	uint64_t add_delivery(std::shared_ptr<const T> result, unsigned int count = 1);
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
	// Adds the fds of all connections to the read-set
	// and kills faulty connections
	int setup_fdset( fd_set *readfds, fd_set* write_fds);

	// Processes the active connections:
	// Checks for data to read/write and takes the corresponding actions
	void process_connections(fd_set *readfds, fd_set* write_fds);


	void handle_cache_request( DeliveryConnection &con );

	void handle_move_request( DeliveryConnection &con );

	void handle_move_done( DeliveryConnection &con );

	// Fetches the delivery with the given id from the internal
	// map and sends it. Throws std::out_of_range if no delivery is present
	// for the given id
	Delivery& get_delivery(uint64_t id);

	// Removes expired deliveries
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

	NodeCacheManager &manager;
};

#endif /* DELIVERY_H_ */
