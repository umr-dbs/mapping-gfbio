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
	NodeCacheEntry( uint64_t entry_id, std::shared_ptr<EType> result, uint64_t size );
	const uint64_t entry_id;
	const std::shared_ptr<EType> data;

	std::string to_string() const;
};

//
// The cache used on nodes.
// To be instantiated by entry-type
//
template<typename EType>
class NodeCache {
public:
	NodeCache( size_t max_size );
	virtual ~NodeCache();

	// Adds an entry for the given semantic_id to the cache.
	const NodeCacheRef put( const std::string &semantic_id, const std::unique_ptr<EType> &item);

	// Removes the entry with the given key from the cache
	void remove( const NodeCacheKey &key );

	// Retrieves the entry with the given key. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const NodeCacheKey &key ) const;

	// Retrieves the entry with the given key. Cannot be modified.
	const std::shared_ptr<EType> get( const NodeCacheKey &key ) const;

	// returns meta-information about the entry for the given key
	const NodeCacheRef get_entry_metadata(const NodeCacheKey& key) const;

	// Queries the cache with the given query-rectangle
	CacheQueryResult<uint64_t> query( const std::string &semantic_id, const QueryRectangle &qr ) const;

	// Returns references to all entries
	std::vector<NodeCacheRef> get_all() const;

	// Returns stats for entries accessed since the last
	// call to this method;
	CacheStats get_stats() const;

	// Returns the maximum size (in bytes) this cache may hold
	size_t get_max_size() const { return max_size; }
	// Returns the current size (in bytes) of all entries stored in the cache
	size_t get_current_size() const { return current_size; }
protected:
	// Copies the content of the entry
	virtual std::unique_ptr<EType> copy(const EType &content) const = 0;
	virtual size_t get_data_size(const EType &content) const = 0;
private:
	typedef CacheStructure<uint64_t,NodeCacheEntry<EType>> Struct;

	std::shared_ptr<NodeCacheEntry<EType>> create_entry( uint64_t id, const EType &data );

	// Retrieves the cache-structure for the given semantic_id.
	Struct* get_structure( const std::string &semantic_id, bool create = false) const;

	void track_access( const NodeCacheKey &key, NodeCacheEntry<EType> &e ) const;

	// Holds the maximum size (in bytes) this cache may hold
	size_t max_size;
	// Holds the current size (in bytes) of all entries stored in the cache
	std::atomic_ullong current_size;
	std::atomic_ullong next_id;

	//std::unique_ptr<ReplacementPolicy<EType>> policy;
	// Holds all cache-structures accessable by the semantic_id
	mutable std::unordered_map<std::string,Struct*> caches;
	mutable std::mutex mtx;
	mutable std::mutex access_mtx;
	mutable std::unordered_map<std::string,std::set<uint64_t>> access_tracker;
};

//
// Raster Cache
//

class NodeRasterCache : public NodeCache<GenericRaster> {
public:
	NodeRasterCache() = delete;
	NodeRasterCache( size_t size );
	virtual ~NodeRasterCache();
protected:
	virtual std::unique_ptr<GenericRaster> copy(const GenericRaster &content) const;
	virtual size_t get_data_size(const GenericRaster &content) const ;
};

#endif /* NODE_CACHE_H_ */
