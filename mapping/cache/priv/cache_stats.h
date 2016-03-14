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
#include <unordered_map>

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
	void toStream( BinaryWriteBuffer &buffer ) const;

	uint64_t entry_id;
	uint64_t last_access;
	uint32_t access_count;
};

class HandshakeEntry : public CacheEntry {
public:
	HandshakeEntry(uint64_t entry_id, const CacheEntry &entry );
	HandshakeEntry( BinaryReadBuffer &buffer );
	void toStream( BinaryWriteBuffer &buffer ) const;

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
	void toStream( BinaryWriteBuffer &buffer ) const;

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
	void toStream( BinaryWriteBuffer &buffer ) const;

	/**
	 * Add a specific item
	 * @param semantic_id the semantic id
	 * @param item the item to add
	 */
	void add_item( const std::string &semantic_id, T item );

	/**
	 * Retrieves all statistic-updates
	 */
	const std::unordered_map<std::string,std::vector<T>>& get_items() const;
private:
	std::unordered_map<std::string,std::vector<T>> items;
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
	void toStream( BinaryWriteBuffer &buffer ) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

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
	void toStream( BinaryWriteBuffer &buffer ) const;


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
	void toStream( BinaryWriteBuffer &buffer ) const;

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
