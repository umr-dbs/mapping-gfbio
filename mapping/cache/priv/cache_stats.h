/*
 * cache_stats.h
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#ifndef CACHE_STATS_H_
#define CACHE_STATS_H_

#include "cache/priv/shared.h"
#include "util/binarystream.h"

#include <vector>
#include <map>

/**
 * This class holds access information about a cache entry
 */
class NodeEntryStats {
public:

	NodeEntryStats( uint64_t id, uint64_t last_access, uint32_t access_count );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	NodeEntryStats( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	uint64_t entry_id;
	uint64_t last_access;
	uint32_t access_count;
};

class HandshakeEntry : public CacheEntry {
public:
	HandshakeEntry(uint64_t entry_id, const CacheEntry &entry );
	HandshakeEntry( BinaryReadBuffer &buffer );
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	uint64_t entry_id;
};

class CacheUsage {
public:
	CacheUsage ( CacheType type, uint64_t capacity_total, uint64_t capacity_used );
	CacheUsage ( BinaryReadBuffer &buffer );

	double get_ratio() const;


	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	CacheType type;
	uint64_t capacity_total;
	uint64_t capacity_used;
};


/**
 * Holds statistics about a single cache - e.g. raster
 */
template<class T>
class CacheContent : public CacheUsage{
protected:
	CacheContent( CacheType type, uint64_t capacity_total, uint64_t capacity_used );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	CacheContent( BinaryReadBuffer &buffer );

public:
	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * Add a specific item
	 * @param semantic_id the semantic id
	 * @param item the item to add
	 */
	void add_item( const std::string &semantic_id, T item );

	/**
	 * Retrieves all statistic-updates
	 */
	const std::map<std::string,std::vector<T>>& get_items() const;
private:
	std::map<std::string,std::vector<T>> items;
};

class CacheStats : public CacheContent<NodeEntryStats> {
public:
	CacheStats( CacheType type, uint64_t capacity_total, uint64_t capacity_used );
	CacheStats( BinaryReadBuffer &buffer );
};

class CacheHandshake : public CacheContent<HandshakeEntry> {
public:
	CacheHandshake( CacheType type, uint64_t capacity_total, uint64_t capacity_used );
	CacheHandshake( BinaryReadBuffer &buffer );
};


/**
 * Holds statistics about cache-queries
 */
class QueryStats {
public:
	QueryStats();

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	QueryStats( BinaryReadBuffer &buffer );

	/**
	 * Adds the given stats and returns a new instance
	 * @param stats the stats to add
	 * @return the cumulated stats
	 */
	QueryStats operator+( const QueryStats& stats ) const;

	/**
	 * Adds the given stats to this instance
	 * @param stats the stats to add
	 * @return this instance
	 */
	QueryStats& operator+=( const QueryStats& stats );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	void add_query( double ratio );

	double get_hit_ratio() const;

	/**
	 * Resets this stats (setting all counts to 0)
	 */
	void reset();

	uint32_t single_local_hits;
	uint32_t multi_local_hits;
	uint32_t multi_local_partials;
	uint32_t single_remote_hits;
	uint32_t multi_remote_hits;
	uint32_t multi_remote_partials;
	uint32_t misses;

	uint64_t result_bytes;

protected:
	size_t queries;
	double ratios;
};

/**
 * Statistics about cache-queries on the index-server
 */
class SystemStats : public QueryStats {
public:
	SystemStats();

	SystemStats( BinaryReadBuffer &buffer );

	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	/**
	 * Resets this stats (setting all counts to 0)
	 */
	void reset();

	uint32_t get_queries_scheduled();
	void query_finished( uint64_t wait_time, uint64_t exec_time );
	void scheduled( uint32_t node_id, uint64_t num_clients = 1 );
	void issued();
	void add_reorg_cycle( uint64_t duration );

private:
	uint32_t queries_issued;
	uint32_t queries_scheduled;
	uint32_t query_counter;
	uint32_t reorg_cycles;

	double max_reorg_time;
	double min_reorg_time;
	double avg_reorg_time;

	double max_wait_time;
	double min_wait_time;
	double avg_wait_time;

	double max_exec_time;
	double min_exec_time;
	double avg_exec_time;

	double max_time;
	double min_time;
	double avg_time;
	std::map<uint32_t,uint64_t> node_to_queries;
};



/**
 * Statistics retrieved by the index-server for each node
 * Contains delta of accessed entries and query-statistics
 */
class NodeStats {
public:
	NodeStats( const QueryStats &query_stats, std::vector<CacheStats> &&stats );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	NodeStats( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;


	QueryStats query_stats;
	std::vector<CacheStats> stats;
};

/**
 * Information send on handshake with the index-server.
 * Contains current memory usage as well as infos about currently cached items.
 */
class NodeHandshake {
public:
	NodeHandshake( uint32_t port, std::vector<CacheHandshake> &&entries );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	NodeHandshake( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return The entries held by the cache
	 */
	const std::vector<CacheHandshake>& get_data() const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	uint32_t port;

private:
	std::vector<CacheHandshake> data;
};


#endif /* CACHE_STATS_H_ */
