/*
 * delivery.h
 *
 *  Created on: 03.07.2015
 *      Author: mika
 */

#ifndef DELIVERY_H_
#define DELIVERY_H_

#include "cache/node/node_manager.h"
#include "cache/node/node_config.h"
#include "cache/priv/connection.h"
#include "operators/operator.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
#include <map>

/**
 * Represents to to deliver response
 * Holds the item itself, a counter representing
 * the number of times this may be sent as well
 * as a timestamp representing its expiration-time.
 */
class Delivery {
public:
	/**
	 * Creates a new instance
	 * @param id the id of this delivery
	 * @param count the number of times this deliverable may be requested
	 * @param raster the data to deliver
	 * @param expiration_time the time this delivery expires
	 */
	Delivery( uint64_t id, uint16_t count, std::shared_ptr<const GenericRaster> raster, time_t expiration_time );

	/**
	 * Creates a new instance
	 * @param id the id of this delivery
	 * @param count the number of times this deliverable may be requested
	 * @param points the data to deliver
	 * @param expiration_time the time this delivery expires
	 */
	Delivery( uint64_t id, uint16_t count, std::shared_ptr<const PointCollection> points, time_t expiration_time );

	/**
	 * Creates a new instance
	 * @param id the id of this delivery
	 * @param count the number of times this deliverable may be requested
	 * @param lines the data to deliver
	 * @param expiration_time the time this delivery expires
	 */
	Delivery( uint64_t id, uint16_t count, std::shared_ptr<const LineCollection> lines, time_t expiration_time );

	/**
	 * Creates a new instance
	 * @param id the id of this delivery
	 * @param count the number of times this deliverable may be requested
	 * @param polygons the data to deliver
	 * @param expiration_time the time this delivery expires
	 */
	Delivery( uint64_t id, uint16_t count, std::shared_ptr<const PolygonCollection> polygons, time_t expiration_time );

	/**
	 * Creates a new instance
	 * @param id the id of this delivery
	 * @param count the number of times this deliverable may be requested
	 * @param plot the data to deliver
	 * @param expiration_time the time this delivery expires
	 */
	Delivery( uint64_t id, uint16_t count, std::shared_ptr<const GenericPlot> plot, time_t expiration_time );

	Delivery( const Delivery &d ) = delete;
	Delivery( Delivery &&d ) = default;

	Delivery& operator=(const Delivery &d) = delete;
	Delivery& operator=(Delivery &&d) = delete;

	/** id the id of this delivery */
	const uint64_t id;

	/** The point in time, this delivery expires */
	const time_t expiration_time;

	/** the number of times this deliverable may be requested */
	uint16_t count;

	/**
	 * Triggers sending this delivery
	 * @param connection the connection to use
	 */
	void send( DeliveryConnection &connection );
private:
	CacheType type;
	std::shared_ptr<const GenericRaster> raster;
	std::shared_ptr<const PointCollection> points;
	std::shared_ptr<const LineCollection> lines;
	std::shared_ptr<const PolygonCollection> polygons;
	std::shared_ptr<const GenericPlot> plot;
};



/**
 * Outsourced delivery-part of the node server-
 * Currently single threaded. Waits for incomming
 * connections and delivers the requested delivery-id
 */
class DeliveryManager {
	friend class std::thread;
	friend class DeliveryConnection;
public:
	/**
	 * Creates a new instance
	 * @param listen_port the port to listen on
	 * @param manager the cache-manager used for direct requests
	 */
	DeliveryManager(const NodeConfig &config, NodeCacheManager &manager);
	~DeliveryManager();

	/**
	 * Adds the given result to the delivery queue.
	 * The returned id must be used by clients fetching
	 * the stored result.
	 * @param result the data to deliver
	 * @param count the number of possible requests to the delivery
	 * @return the unique delivery-id used for retrieving the data
	 */
	template <typename T>
	uint64_t add_delivery(std::shared_ptr<const T> result, uint32_t count = 1);

	/**
	 * Fires up the delivery-manager and will return after
	 * stop() is invoked by another thread
	 */
	void run();

	/**
	 * Fires up the delivery-manager in a separate thread
	 * and returns it.
	 * @return the thread running this delivery-manager instance
	 */
	std::unique_ptr<std::thread> run_async();

	/**
	 * Triggers the shutdown of the node-server
	 * Subsequent calls to run or run_async have undefined
	 * behaviour.
	 */
	void stop();
private:

	/**
	 * Processes the handshake with newly accepted connections
	 * @param new_fds the accepted but not initialized connections
	 * @param readfds the set of fds
	 */
	void process_handshake( std::vector<std::unique_ptr<NewNBConnection>> &new_fds );

	/**
	 * Processes all active connections. Checks if socket is ready
	 * to send/receive data and takes the appropriate actions
	 * @param readfds the fd_set for reads
	 * @param writefds the fd_set for writes
	 */
	void process_connections();

	/**
	 * Processes direct requests to a cache-entry.
	 * @param con the connection, the request was submitted with
	 */
	void handle_cache_request( DeliveryConnection &con );

	/**
	 * Processes migration requests
	 * @param con the connection, the request was submitted with
	 */
	void handle_move_request( DeliveryConnection &con );

	/**
	 * Processes confirmations of entry-migration
	 * @param con the connection, the request was submitted with
	 */
	void handle_move_done( DeliveryConnection &con );

	/**
	 * @param id the id of the delivery to retrieve
	 * @return the delivery for the given id
	 */
	Delivery& get_delivery(uint64_t id);

	/**
	 * Removes all expired deliveries from the internal list.
	 * These are all deliveries where the max. number of requests
	 * is reached, or where the timeout is reached.
	 */
	void remove_expired_deliveries();

	void wakeup();

	/** Indicator telling if the manager should shutdown */
	bool shutdown;

	NodeConfig config;

	/** the mutex used to acces the stored deliveries */
	std::mutex delivery_mutex;

	/** The counter for the delivery-ids */
	uint64_t delivery_id;

	/** the currently stored deliveries */
	std::map<uint64_t, Delivery> deliveries;

	/**  the currently open connections */
	std::vector<std::unique_ptr<DeliveryConnection>> connections;

	/** Reference to the cache-manager */
	NodeCacheManager &manager;

	BinaryStream wakeup_pipe;
};

#endif /* DELIVERY_H_ */
