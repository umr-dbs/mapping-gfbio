/*
 * node.h
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#ifndef INDEX_NODE_H_
#define INDEX_NODE_H_

#include "cache/priv/cache_stats.h"
#include "cache/priv/requests.h"
#include "cache/priv/redistribution.h"
#include "cache/priv/connection.h"

class QueryManager;

/**
 * Models a cache-node
 */
class Node {
public:
	Node(uint32_t id, const std::string &host, const NodeHandshake &hs, std::unique_ptr<ControlConnection> cc );


	const CacheUsage& get_usage(  CacheType type ) const;

	/**
	 * Updates the statistics of this node
	 * @param stats the statistics delta
	 */
	void update_stats( const NodeStats &stats );

	/**
	 * @return the query-stats of this node
	 */
	const QueryStats& get_query_stats() const;

	/**
	 * Resets the query-statistics of this node
	 */
	void reset_query_stats();

	/**
	 * @return a human readable representation
	 */
	std::string to_string() const;

	void setup_connections(struct pollfd *fds, size_t &pos, QueryManager &query_manager);

	time_t last_stats_request() const;

	bool is_control_connection_idle() const;

	void send_stats_request();

	void send_reorg( const ReorgDescription &desc );

	void add_worker( std::unique_ptr<WorkerConnection> worker );

	bool has_idle_worker() const;

	uint32_t num_idle_workers() const;

	uint64_t schedule_request( uint8_t cmd, const BaseRequest &req );

	void release_worker( uint64_t id );

	ControlConnection& get_control_connection();

	std::map<uint64_t,std::unique_ptr<WorkerConnection>>& get_busy_workers();


	/** The unique id of this node */
	const uint32_t id;
	/** The hostname of this node */
	const std::string host;
	/** The port for delivery connections on this node */
	const uint32_t port;
	/** The id of the control-connection */
//	uint64_t control_connection;

private:
	std::unique_ptr<ControlConnection> control_connection;
	std::vector<std::unique_ptr<WorkerConnection>> idle_workers;
	std::map<uint64_t,std::unique_ptr<WorkerConnection>> busy_workers;

	/** The timestamp of the last stats request */
	time_t _last_stats_request;

	// The stats
	std::map<CacheType,CacheUsage> usage;
	QueryStats query_stats;
};



#endif /* INDEX_NODE_H_ */
