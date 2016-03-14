/*
 * node_cache.h
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#ifndef NODE_CACHE_H_
#define NODE_CACHE_H_

#include "cache/priv/cache_structure.h"
#include "cache/priv/cache_stats.h"

#include <vector>
#include <unordered_map>
#include <set>

#include <atomic>
#include <memory>
#include <mutex>


/**
 * Models an entry in the node-cache
 */
template<typename EType>
class NodeCacheEntry : public CacheEntry {
public:
	/**
	 * Creates a new instance.
	 * @param entry_id the unique id of this entry
	 * @param meta the meta-information
	 * @param result the data to cache
	 *
	 */
	NodeCacheEntry( uint64_t entry_id, const CacheEntry &meta, std::shared_ptr<EType> result );

	/**
	 * @return a copy of the cached data
	 */
	std::unique_ptr<EType> copy_data() const;

	/**
	 * @return a human readable representation
	 */
	std::string to_string() const;

	const uint64_t entry_id;
	const std::shared_ptr<const EType> data;
};

/**
 * Cache implementation on the node side.
 * Additionally keeps track of access to the entries and
 * used capacity.
 */
template<typename EType>
class NodeCache : public Cache<uint64_t,NodeCacheEntry<EType>> {
public:
	/**
	 * Creates a new instance
	 * @param type the type of the cached items
	 * @param max_size the max. size this cache may use (in bytes)
	 */
	NodeCache( CacheType type, size_t max_size );

	NodeCache() = delete;
	NodeCache( const NodeCache& ) = delete;

	/**
	 * Adds an entry to the cache. The given data-item is cloned and stored.
	 * @param semantic_id the semantic id
	 * @param item the data-item to cache
	 * @param meta the meta-data
	 * @return the meta-data of the newly created entry including its unique id
	 */
	const MetaCacheEntry put( const std::string &semantic_id, const std::unique_ptr<EType> &item, const CacheEntry &meta);

	/**
	 * Removes the entry with the given key
	 * @param key the key of the entry to remove
	 */
	void remove( const NodeCacheKey &key );

	/**
	 * Retrieves the cached item with the given key
	 * @param key the key of the item to retrieve
	 * @return the item with the given key
	 */
	std::shared_ptr<const NodeCacheEntry<EType>> get( const NodeCacheKey &key ) const;

	/**
	 * Retrieves the meta-data for all items currently stored
	 * in the cache. This operation does not affect the access-statistics
	 */
	CacheHandshake get_all() const;

	// Returns stats for entries accessed since the last
	// call to this method;
	/**
	 * Retrieves the delta-statistics for all entries accessed since
	 * the last call to this method.
	 * @return the access-statistics for all entries accessed since the last call to this method
	 */
	CacheStats get_stats() const;

	/**
	 * @return the maximum size (in bytes) this cache may hold
	 */
	size_t get_max_size() const { return max_size; }

	/**
	 * @return the current size (in bytes) of all entries stored in the cache
	 */
	size_t get_current_size() const { return current_size; }

	/** The type of the cached items */
	const CacheType type;
private:
	/**
	 * Increases the access-count of for the entry with the given key and
	 * sets the last access timestamp to the current time.
	 * @param key the key of the accessed entry
	 * @param e the entry
	 */
	void track_access( const NodeCacheKey &key, NodeCacheEntry<EType> &e ) const;

	/** Holds the maximum size (in bytes) this cache may hold */
	size_t max_size;

	/** Holds the current size (in bytes) of all entries stored in the cache */
	std::atomic_ullong current_size;

	/** The next unique id */
	std::atomic_ullong next_id;

	/** Mutex used during access-tracking */
	mutable std::mutex access_mtx;

	/** Collects the ids of all accessed entries -- used for delta-statistics */
	mutable std::unordered_map<std::string,std::set<uint64_t>> access_tracker;
};


#endif /* NODE_CACHE_H_ */
