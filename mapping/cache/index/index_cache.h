/*
 * index_cache.h
 *
 *  Created on: 07.08.2015
 *      Author: mika
 */

#ifndef INDEX_CACHE_H_
#define INDEX_CACHE_H_

#include "cache/priv/cache_structure.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/common.h"
#include <utility>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>

class Node;
class NodeReorgDescription;
class ReorgStrategy;

//
// Key for index entries
// Basically the same as on a node, but with
// an additional node_id.
//
class IndexCacheKey : public NodeCacheKey {
public:
	IndexCacheKey( const std::string &semantic_id, std::pair<uint32_t,uint64_t> id );
	IndexCacheKey( uint32_t node_id, const std::string &semantic_id, uint64_t entry_id );

	std::string to_string() const;

	uint32_t node_id;
};

//
// An entry on the index-cache.
//
class IndexCacheEntry : public IndexCacheKey, public CacheEntry {
public:
	IndexCacheEntry( uint32_t node_id, const NodeCacheRef &ref  );
};

//
// Cache on the index-server.
// One instance used per data-type.
//
class IndexCache {
public:
	// Constructs a new instance with the given reorg-strategy
	IndexCache( const std::string &reorg_strategy );
	virtual ~IndexCache() = default;

	IndexCache() = delete;
	IndexCache( const IndexCache& ) = delete;
	IndexCache( IndexCache&& ) = delete;

	// Adds an entry for the given semantic_id to the cache.
	void put( const std::shared_ptr<IndexCacheEntry> &entry );

	// Retrieves the entry with the given key.
	std::shared_ptr<const IndexCacheEntry> get( const IndexCacheKey &key ) const;

	// Queries the cache with the given query-rectangle
	CacheQueryResult<std::pair<uint32_t,uint64_t>> query( const std::string &semantic_id, const QueryRectangle &qr ) const;

	// Adds an entry for the given semantic_id to the cache.
	void remove( const IndexCacheKey &key );

	// Adds an entry for the given semantic_id to the cache.
	void move( const IndexCacheKey &old_key, const IndexCacheKey &new_key );

	// Removes all entries for the given node
	void remove_all_by_node( uint32_t node_id );

	// Retrieves all elements currently stored
	std::vector<std::shared_ptr<const IndexCacheEntry>> get_all() const;

	// Updates statistics of the entries
	void update_stats( uint32_t node_id, const CacheStats &stats );

	// Tells if a global reorganization is required
	bool requires_reorg( const std::map<uint32_t, std::shared_ptr<Node> > &nodes ) const;

	// Calculates an appropriate reorganization
	void reorganize(std::map<uint32_t, NodeReorgDescription>& result );

	// Tells us which node to use for the given query
	uint32_t get_node_for_job( const BaseRequest &query, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;

	// Tells the total capacity of the implementing cache
	virtual size_t get_total_capacity( const Capacity& capacity ) const = 0;
	// Tells the used capacity of the implementing cache
	virtual size_t get_used_capacity( const Capacity& capacity ) const = 0;
	// Tells the usage of the implementing cache
	double get_capacity_usage( const Capacity& capacity ) const;
	// Tells the type of item cached here
	virtual CacheType get_reorg_type() const = 0;

private:
	typedef CacheStructure<std::pair<uint32_t,uint64_t>,IndexCacheEntry> Struct;

	// Retrieves the cache-structure for the given semantic_id.
	Struct* get_structure( const std::string &semantic_id, bool create = false) const;

	// Gets all entries for the given node
	std::set<std::shared_ptr<const IndexCacheEntry>> &get_node_entries(uint32_t node_id) const;

	// Removes an entry from the corresponding node-list
	void remove_from_node( const std::shared_ptr<IndexCacheEntry> &e );

	// Holds a reference to all entries clustered by node
	mutable std::map<uint32_t, std::set<std::shared_ptr<const IndexCacheEntry>>> entries_by_node;

	// Holds all cache-structures accessable by the semantic_id
	mutable std::unordered_map<std::string,Struct*> caches;

	// The reorganization strategy
	std::unique_ptr<ReorgStrategy> reorg_strategy;
};

class IndexRasterCache : public IndexCache {
public:
	IndexRasterCache(const std::string &reorg_strategy);
	IndexRasterCache() = delete;
	IndexRasterCache( const IndexRasterCache& ) = delete;
	IndexRasterCache( IndexRasterCache&& ) = delete;

	size_t get_total_capacity( const Capacity& capacity ) const;
	size_t get_used_capacity( const Capacity& capacity ) const;
	CacheType get_reorg_type() const;
};

class IndexPointCache : public IndexCache {
public:
	IndexPointCache(const std::string &reorg_strategy);
	IndexPointCache() = delete;
	IndexPointCache( const IndexPointCache& ) = delete;
	IndexPointCache( IndexPointCache&& ) = delete;
	size_t get_total_capacity( const Capacity& capacity ) const;
	size_t get_used_capacity( const Capacity& capacity ) const;
	CacheType get_reorg_type() const;
};

class IndexLineCache : public IndexCache {
public:
	IndexLineCache(const std::string &reorg_strategy);
	IndexLineCache() = delete;
	IndexLineCache( const IndexLineCache& ) = delete;
	IndexLineCache( IndexLineCache&& ) = delete;
	size_t get_total_capacity( const Capacity& capacity ) const;
	size_t get_used_capacity( const Capacity& capacity ) const;
	CacheType get_reorg_type() const;
};

class IndexPolygonCache : public IndexCache {
public:
	IndexPolygonCache(const std::string &reorg_strategy);
	IndexPolygonCache() = delete;
	IndexPolygonCache( const IndexPolygonCache& ) = delete;
	IndexPolygonCache( IndexPolygonCache&& ) = delete;
	size_t get_total_capacity( const Capacity& capacity ) const;
	size_t get_used_capacity( const Capacity& capacity ) const;
	CacheType get_reorg_type() const;
};

class IndexPlotCache : public IndexCache {
public:
	IndexPlotCache(const std::string &reorg_strategy);
	IndexPlotCache() = delete;
	IndexPlotCache( const IndexPlotCache& ) = delete;
	IndexPlotCache( IndexPlotCache&& ) = delete;
	size_t get_total_capacity( const Capacity& capacity ) const;
	size_t get_used_capacity( const Capacity& capacity ) const;
	CacheType get_reorg_type() const;
};

class IndexCaches {
public:
	IndexCaches( const std::string &reorg_strategy );
	IndexCaches() = delete;
	IndexCaches( const IndexCaches& ) = delete;
	IndexCaches( IndexCaches&& ) = delete;
	IndexCaches& operator=( const IndexCaches& ) = delete;
	IndexCaches& operator=( IndexCaches&& ) = delete;

	IndexCache& get_cache( CacheType type );

	void process_handshake( uint32_t node_id, const NodeHandshake &hs) ;
	bool require_reorg(const std::map<uint32_t, std::shared_ptr<Node> > &nodes) const;
	void remove_all_by_node( uint32_t node_id );

	void update_stats( uint32_t node_id, const NodeStats& stats );
	void reorganize(const std::map<uint32_t, std::shared_ptr<Node> > &nodes, std::map<uint32_t, NodeReorgDescription>& result );

private:
	std::vector<std::reference_wrapper<IndexCache>> all_caches;
	IndexRasterCache  raster_cache;
	IndexPointCache   point_cache;
	IndexLineCache    line_cache;
	IndexPolygonCache poly_cache;
	IndexPlotCache    plot_cache;
};

#endif /* INDEX_CACHE_H_ */
