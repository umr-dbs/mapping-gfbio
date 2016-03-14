/*
 * node.h
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#ifndef INDEX_NODE_H_
#define INDEX_NODE_H_

#include "cache/priv/cache_stats.h"

/**
 * Models a cache-node
 */
class Node {
public:
	Node(uint32_t id, const std::string &host, const NodeHandshake &hs);


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


	/** The unique id of this node */
	const uint32_t id;
	/** The hostname of this node */
	const std::string host;
	/** The port for delivery connections on this node */
	const uint32_t port;
	/** The timestamp of the last stats update */
	time_t last_stat_update;
	/** The id of the control-connection */
	uint64_t control_connection;

private:
	// The stats
	std::map<CacheType,CacheUsage> usage;
	QueryStats query_stats;
};



#endif /* INDEX_NODE_H_ */
