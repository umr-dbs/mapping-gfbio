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
#include <utility>
#include <map>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>

class ReorgStrategy;
class Node;
class NodeReorgDescription;

//
// Key for index entries
// Basically the same as on a node, but with
// an additional node_id.
//
class IndexCacheKey : public NodeCacheKey {
public:
	IndexCacheKey( const std::string &semantic_id, std::pair<uint32_t,uint64_t> id );
	IndexCacheKey( uint32_t node_id, const std::string &semantic_id, uint64_t entry_id );
	uint32_t node_id;
};

//
// An entry on the index-cache.
//
class IndexCacheEntry : public IndexCacheKey, public CacheEntry {
public:
	IndexCacheEntry( uint32_t node_id, NodeCacheRef ref  );
};

//
// Cache on the index-server.
// One instance used per data-type.
//
class IndexCache {
public:
	// Constructs a new instance with the given reorg-strategy
	IndexCache( ReorgStrategy &strategy );

	// Adds an entry for the given semantic_id to the cache.
	void put( const IndexCacheEntry &entry );

	// Retrieves the entry with the given key.
	const IndexCacheEntry& get( const IndexCacheKey &key ) const;

	// Queries the cache with the given query-rectangle
	CacheQueryResult<std::pair<uint32_t,uint64_t>> query( const std::string &semantic_id, const QueryRectangle &qr ) const;

	// Adds an entry for the given semantic_id to the cache.
	void remove( const IndexCacheKey &key );

	// Adds an entry for the given semantic_id to the cache.
	void move( const IndexCacheKey &old_key, const IndexCacheKey &new_key );

	// Gets all entries for the given node
	std::vector<std::shared_ptr<IndexCacheEntry>> &get_node_entries(uint32_t node_id) const;

	// Removes all entries for the given node
	void remove_all_by_node( uint32_t node_id );

	// Tells if a global reorganization is required
	bool requires_reorg( const std::map<uint32_t, std::shared_ptr<Node> > &nodes );

	// Updates statistics of the entries
	void update_stats( uint32_t node_id, const CacheStats &stats );

	// Calculates an appropriate reorganization
	std::vector<NodeReorgDescription> reorganize(const std::map<uint32_t,std::shared_ptr<Node>> &nodes );

private:
	typedef CacheStructure<std::pair<uint32_t,uint64_t>,IndexCacheEntry> Struct;

	// Retrieves the cache-structure for the given semantic_id.
	Struct* get_structure( const std::string &semantic_id, bool create = false) const;

	// Removes an entry from the corresponding node-list
	void remove_from_node( const IndexCacheKey &key );

	// Holds a reference to all entries clustered by node
	mutable std::map<uint32_t, std::vector<std::shared_ptr<IndexCacheEntry>>> entries_by_node;

	// Holds all cache-structures accessable by the semantic_id
	mutable std::unordered_map<std::string,Struct*> caches;

	// The reorganization strategy
	ReorgStrategy& reorg_strategy;
};
#endif /* INDEX_CACHE_H_ */
