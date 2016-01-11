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
#include "operators/queryrectangle.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"
#include "util/binarystream.h"

#include <geos/geom/Polygon.h>

#include <atomic>
#include <vector>
#include <unordered_map>
#include <set>
#include <memory>
#include <mutex>


//
// An entry on the node-cache
//
template<typename EType>
class NodeCacheEntry : public CacheEntry {
public:
	NodeCacheEntry( uint64_t entry_id, const CacheEntry &meta, std::shared_ptr<EType> result );
	const uint64_t entry_id;
	const std::shared_ptr<EType> data;

	std::string to_string() const;
};

//
// The cache used on nodes.
// To be instantiated by entry-type
//
template<typename EType>
class NodeCache : public Cache<uint64_t,NodeCacheEntry<EType>> {
public:
	NodeCache( CacheType type, size_t max_size );
	NodeCache() = delete;
	NodeCache( const NodeCache& ) = delete;

	// Adds an entry for the given semantic_id to the cache.
	const NodeCacheRef put( const std::string &semantic_id, const std::unique_ptr<EType> &item, const CacheEntry &meta);

	// Removes the entry with the given key from the cache
	void remove( const NodeCacheKey &key );

	// Retrieves the entry with the given key. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const NodeCacheKey &key ) const;

	// Retrieves the entry with the given key. Cannot be modified.
	const std::shared_ptr<const EType> get( const NodeCacheKey &key ) const;

	// returns meta-information about the entry for the given key
	const NodeCacheRef get_entry_metadata(const NodeCacheKey& key) const;

	// Returns references to all entries
	std::vector<NodeCacheRef> get_all() const;

	// Returns stats for entries accessed since the last
	// call to this method;
	CacheStats get_stats() const;

	// Returns the maximum size (in bytes) this cache may hold
	size_t get_max_size() const { return max_size; }
	// Returns the current size (in bytes) of all entries stored in the cache
	size_t get_current_size() const { return current_size; }

	const CacheType type;
private:
	// Tracks the access to the given item
	void track_access( const NodeCacheKey &key, NodeCacheEntry<EType> &e ) const;

	// Holds the maximum size (in bytes) this cache may hold
	size_t max_size;
	// Holds the current size (in bytes) of all entries stored in the cache
	std::atomic_ullong current_size;
	std::atomic_ullong next_id;

	// Mutex used during access-tracking
	mutable std::mutex access_mtx;
	// Collects the ids of all accessed entries
	mutable std::unordered_map<std::string,std::set<uint64_t>> access_tracker;
};

#endif /* NODE_CACHE_H_ */
