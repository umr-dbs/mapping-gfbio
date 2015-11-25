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
	NodeHandshake( const std::string &host, uint32_t port, const Capacity &capacity, std::vector<NodeCacheRef> entries );
	NodeHandshake( BinaryStream &stream );

	const std::vector<NodeCacheRef>& get_entries() const;

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	std::string host;
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


//
// Holds an incremental list of access stats
//

class NodeStats : public Capacity {
public:
	NodeStats( const Capacity &capacity, std::vector<CacheStats> stats );
	NodeStats( BinaryStream &stream );
	void toStream( BinaryStream &stream ) const;
	std::vector<CacheStats> stats;
};




#endif /* CACHE_STATS_H_ */
