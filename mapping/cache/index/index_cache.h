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
#include "cache/priv/redistribution.h"
#include "cache/priv/requests.h"
#include "cache/common.h"
#include <utility>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>
#include <mutex>

/**
 * Unique key for an entry in the global cache-index
 */
class IndexCacheKey {
public:
	/**
	 * Constructs a new instance
	 * @param semantic_id the semantic id
	 * @param id the unique id consisting of the node's id and the entry id
	 */
	IndexCacheKey( const std::string &semantic_id, const std::pair<uint32_t,uint64_t> &id );

	/**
	 * Constructs a new instance
	 * @param semantic_id the semantic id
	 * @param node_id the id of the node hosting this entry
	 * @param entry_id the id of the entry
	 */
	IndexCacheKey( const std::string &semantic_id, uint32_t node_id, uint64_t entry_id );

	bool operator<( const IndexCacheKey &l ) const;
	bool operator==( const IndexCacheKey &l ) const;

	/**
	 * @return the id of the node hosting this entry
	 */
	uint32_t get_node_id() const;

	/**
	 * @return the id of the entry
	 */
	uint64_t get_entry_id() const;

	/**
	 * @return a human readable representation
	 */
	std::string to_string() const;

	std::string semantic_id;
	std::pair<uint32_t,uint64_t> id;
};

/**
 * Entry in the index-cache
 */
class IndexCacheEntry : public CacheEntry {
	friend class IndexCache;
private:
	IndexCacheEntry( const std::string &semantic_id, uint32_t node_id, uint64_t entry_id, const CacheEntry &ref  );
public:

	/**
	 * @return the id of the node hosting this entry
	 */
	uint32_t get_node_id() const;

	/**
	 * @return the id of the entry
	 */
	uint64_t get_entry_id() const;

	const std::string &semantic_id;
//	std::string semantic_id;
	std::pair<uint32_t,uint64_t> id;
};

/**
 * Cache implementation for the index-server.
 */
class IndexCache : public Cache<std::pair<uint32_t,uint64_t>,IndexCacheEntry> {
public:
	/**
	 * Constructs a new instance
	 * @param type the data-item type
	 */
	IndexCache( CacheType type );
	virtual ~IndexCache() = default;

	IndexCache() = delete;
	IndexCache( const IndexCache& ) = delete;
	IndexCache( IndexCache&& ) = delete;

	/**
	 * Adds the given entry
	 * @param entry the entry to add
	 */
	void put( const std::string &semantic_id, uint32_t node_id, uint64_t entry_id, const CacheEntry& entry );


	/**
	 * Retrieves the entry with the given key
	 * @param key the entry's key
	 * @return the entry for the given key
	 */
	std::shared_ptr<const IndexCacheEntry> get( const IndexCacheKey &key ) const;

	/**
	 * Removes the entry with the given key
	 * @param key the entry's key
	 */
	void remove( const IndexCacheKey &key );

	// Adds an entry for the given semantic_id to the cache.
	/**
	 * Moves the entry described by key 1 to key 2. Used to migrate entries
	 * from one node to another.
	 * @param old_key the current key of an entry
	 * @param new_key the new key the entry should be stored with
	 */
	void move( const IndexCacheKey &old_key, const IndexCacheKey &new_key );

	/**
	 * Removes all entries hosted on the node with the given id
	 * @param node_id the id of the node
	 */
	void remove_all_by_node( uint32_t node_id );

	/**
	 * @return all entries currently stored in the cache
	 */
	std::vector<std::shared_ptr<const IndexCacheEntry>> get_all() const;

	// Updates statistics of the entries
	/**
	 * Update the statistics of entries from the given node
	 * @param node_id the id of the node, which sent the stats
	 * @param stats the stats-delta
	 */
	void update_stats( uint32_t node_id, const CacheStats &stats );

	const CacheType type;
private:
	// Gets all entries for the given node
	/**
	 * Retrieves all entries hosted on the node with the given id
	 * @param node_id the id of the node
	 * @return all entries hosted on the node with the given id
	 */
	std::set<std::shared_ptr<const IndexCacheEntry>> &get_node_entries(uint32_t node_id) const;

	/**
	 * Removes an entry from the per-node list
	 * @param e the entry to remove
	 */
	void remove_from_node( const std::shared_ptr<IndexCacheEntry> &e );

	std::unordered_set<std::string> semantic_ids;

	// Holds a reference to all entries clustered by node
	mutable std::map<uint32_t, std::set<std::shared_ptr<const IndexCacheEntry>>> entries_by_node;
};

#endif /* INDEX_CACHE_H_ */
