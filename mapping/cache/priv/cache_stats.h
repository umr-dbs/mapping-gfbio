/*
 * cache_stats.h
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#ifndef CACHE_STATS_H_
#define CACHE_STATS_H_

#include "cache/priv/cache_structure.h"
#include "util/binarystream.h"

#include <vector>
#include <unordered_map>

//
// Holds memory usage information of
// the caches
//

class Capacity {
public:
	Capacity( uint64_t raster_cache_total, uint64_t raster_cache_used,
			  uint64_t point_cache_total, uint64_t point_cache_used,
			  uint64_t line_cache_total, uint64_t line_cache_used,
			  uint64_t polygon_cache_total, uint64_t polygon_cache_used,
			  uint64_t plot_cache_total, uint64_t plot_cache_used );
	Capacity( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	std::string to_string() const;

	uint64_t raster_cache_total;
	uint64_t raster_cache_used;
	uint64_t point_cache_total;
	uint64_t point_cache_used;
	uint64_t line_cache_total;
	uint64_t line_cache_used;
	uint64_t polygon_cache_total;
	uint64_t polygon_cache_used;
	uint64_t plot_cache_total;
	uint64_t plot_cache_used;
};

//
// Information send on handshake with index
// Contains current memory usage as well as
// infos about currently cached items
//

class NodeHandshake : public Capacity {
public:
	NodeHandshake( uint32_t port, const Capacity &capacity, std::vector<NodeCacheRef> entries );
	NodeHandshake( BinaryStream &stream );

	const std::vector<NodeCacheRef>& get_entries() const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	uint32_t port;

private:
	std::vector<NodeCacheRef> entries;
};

//
// Stats
//
class NodeEntryStats {
public:
	NodeEntryStats( uint64_t id, time_t last_access, uint32_t access_count );
	NodeEntryStats( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	uint64_t entry_id;
	time_t last_access;
	uint32_t access_count;
};

//
// Holds statistics about a single cache - e.g. raster
//
class CacheStats {
public:
	CacheStats( CacheType type );
	CacheStats( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	void add_stats( const std::string &semantic_id, NodeEntryStats stats );

	const std::unordered_map<std::string,std::vector<NodeEntryStats>>& get_stats() const;

	CacheType type;
private:
	std::unordered_map<std::string,std::vector<NodeEntryStats>> stats;
};


class QueryStats {
public:
	QueryStats();
	QueryStats( BinaryStream &stream );

	QueryStats operator+( const QueryStats& stats ) const;
	QueryStats& operator+=( const QueryStats& stats );

	void toStream( BinaryStream &stream ) const;

	std::string to_string() const;

	void reset();

	uint32_t single_local_hits;
	uint32_t multi_local_hits;
	uint32_t multi_local_partials;
	uint32_t single_remote_hits;
	uint32_t multi_remote_hits;
	uint32_t multi_remote_partials;
	uint32_t misses;
};


class ActiveQueryStats : private QueryStats {
public:
	void add_single_local_hit();
	void add_multi_local_hit();
	void add_multi_local_partial();
	void add_single_remote_hit();
	void add_multi_remote_hit();
	void add_multi_remote_partial();
	void add_miss();

	QueryStats get() const;
	QueryStats get_and_reset();
private:
	mutable std::mutex mtx;
};

//
// Holds an incremental list of access stats
//

class NodeStats : public Capacity {
public:
	NodeStats( const Capacity &capacity, const QueryStats &query_stats, std::vector<CacheStats> stats );
	NodeStats( BinaryStream &stream );
	void toStream( BinaryStream &stream ) const;
	QueryStats query_stats;
	std::vector<CacheStats> stats;
};




#endif /* CACHE_STATS_H_ */
