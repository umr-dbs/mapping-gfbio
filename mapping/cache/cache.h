/*
 * local_cache.h
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

#ifndef CACHE_H_
#define CACHE_H_

#include "cache/types.h"
//#include "cache/replacementpolicy.h"
#include "util/log.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"

#include <string>
#include <memory>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>

class SocketConnection;
class GenericOperator;

template<typename EType>
class STCacheStructure;


//
// Unique key generated for an entry in the cache
//
class STCacheKey {
public:
	STCacheKey( const std::string &semantic_id, uint64_t entry_id );
	STCacheKey( BinaryStream &stream );
	void toStream( BinaryStream &stream ) const;
	std::string semantic_id;
	uint64_t entry_id;
};

//
// Interface describing the structure in which cache-entries
// are saved.
// A call to remove deletes the entry from the cache.
//
template<typename EType>
class STCacheStructure {
public:
	virtual ~STCacheStructure() {};
	// Inserts the given result into the cache. The cache copies the content of the result.
	virtual uint64_t insert( const std::unique_ptr<EType> &result ) = 0;

	// Fetches the entry by the given id and returns a copy
	virtual std::unique_ptr<EType> get_copy( const uint64_t id ) const = 0;
	// Queries this cache with the given rectangle and returns a copy of the result
	// if a match is found
	virtual std::unique_ptr<EType> query_copy( const QueryRectangle &spec ) const = 0;

	// Fetches the entry by the given id. The result is read-only
	// and not copied.
	virtual const std::shared_ptr<EType> get( const uint64_t id ) const = 0;

	// Queries this cache with the given rectangle. The result is read-only
	// and not copied.
	// if a match is found
	virtual const std::shared_ptr<EType> query( const QueryRectangle &spec ) const = 0;

	// Returns the total size of the entry with the given id
	virtual uint64_t get_entry_size( const uint64_t id ) const = 0;
	// Removes the entry with the given id
	virtual void remove( const uint64_t id ) = 0;
};

//
// Abstract cache class. To be completed per Entry-Type
//
template<typename EType>
class STCache {
public:
	STCache( size_t max_size ) : max_size(max_size), current_size(0) {
		Log::debug("Creating new cache with max-size: %d", max_size);
		//policy = std::unique_ptr<ReplacementPolicy<EType>>( new LRUPolicy<EType>() );
	};
	virtual ~STCache();
	// Adds an entry for the given semantic_id to the cache.
	virtual STCacheKey put( const std::string &semantic_id, const std::unique_ptr<EType> &item);
	// Removes the entry with the given key from the cache
	void remove( const STCacheKey &key );
	// Removes the entry with the given semantic_id and entry_id from the cache
	virtual void remove( const std::string &semantic_id, uint64_t entry_id );
	// Retrieves the entry with the given key. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const STCacheKey &key ) const;
	// Retrieves the entry with the given semantic_id and entry_id. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const std::string &semantic_id, uint64_t id ) const;
	// Queries the cache for an entry matching the given semantid_id and query-rectangle. A copy is returned which may be modified
	std::unique_ptr<EType> query_copy( const std::string &semantic_id, const QueryRectangle &qr ) const;

	// Retrieves the entry with the given key. Cannot be modified.
	const std::shared_ptr<EType> get( const STCacheKey &key ) const;
	// Retrieves the entry with the given semantic_id and entry_id.  Cannot be modified.
	const std::shared_ptr<EType> get( const std::string &semantic_id, uint64_t id ) const;
	// Queries the cache for an entry matching the given semantid_id and query-rectangle.  Cannot be modified.
	const std::shared_ptr<EType> query( const std::string &semantic_id, const QueryRectangle &qr ) const;


	// Returns the maximum size (in bytes) this cache may hold
	size_t get_max_size() const { return max_size; }
	// Returns the current size (in bytes) of all entries stored in the cache
	size_t get_current_size() const { return current_size; }
protected:
	// Creates and returns a new structure for holding cache-entries
	// A single structure per semantic_id is used.
	virtual STCacheStructure<EType>* new_structure() const = 0;
private:
	// Retrieves the cache-structure for the given semantic_id.
	STCacheStructure<EType>* get_structure( const std::string &semantic_id, bool create = false) const;
	// Holds the maximum size (in bytes) this cache may hold
	size_t max_size;
	// Holds the current size (in bytes) of all entries stored in the cache
	size_t current_size;
	//std::unique_ptr<ReplacementPolicy<EType>> policy;
	// Holds all cache-structures accessable by the semantic_id
	mutable std::unordered_map<std::string,STCacheStructure<EType>*> caches;
	mutable std::mutex mtx;
};

//
// Raster Cache
//

class RasterCache : public STCache<GenericRaster> {
public:
	RasterCache() = delete;
	RasterCache( size_t size ) : STCache(size) {};
	virtual ~RasterCache() {};
protected:
	virtual STCacheStructure<GenericRaster>* new_structure() const;
};

//
// Index Raster Cache
//

class RasterRefCache : public STCache<RasterRef> {
public:
	RasterRefCache() : STCache(1 << 31) {};
	virtual ~RasterRefCache() {};
	void remove_all_by_node( uint32_t node_id );
	virtual void remove( const std::string &semantic_id, uint64_t id );
	virtual STCacheKey put( const std::string &semantic_id, const std::unique_ptr<RasterRef> &item);
protected:
	virtual STCacheStructure<RasterRef>* new_structure() const;
private:
	std::map<uint32_t,std::vector<STCacheKey>> entries_by_node;
};


//
// Cache-Manager
//

class CacheManager {
public:
	static CacheManager& getInstance();
	static void init( std::unique_ptr<CacheManager> &impl );
	static thread_local SocketConnection *remote_connection;
	virtual ~CacheManager() {};
	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) = 0;
	virtual std::unique_ptr<GenericRaster> get_raster( const std::string &semantic_id, uint64_t entry_id ) = 0;
	virtual std::unique_ptr<GenericRaster> get_raster( const STCacheKey &key );
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster ) = 0;
private:
	static std::unique_ptr<CacheManager> impl;
};

class LocalCacheManager : public CacheManager {
public:
	LocalCacheManager( size_t rasterCacheSize ) : rasterCache(rasterCacheSize) {};
	virtual ~LocalCacheManager() {};
	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) ;
	virtual std::unique_ptr<GenericRaster> get_raster( const std::string &semantic_id, uint64_t entry_id );
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
private:
	RasterCache rasterCache;
};

class NopCacheManager : public CacheManager {
public:
	virtual ~NopCacheManager() {};
	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) ;
	virtual std::unique_ptr<GenericRaster> get_raster( const std::string &semantic_id, uint64_t entry_id );
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
};

class RemoteCacheManager : public CacheManager {
public:
	RemoteCacheManager( size_t rasterCacheSize ) : local_cache(rasterCacheSize) {};
	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) ;
	virtual std::unique_ptr<GenericRaster> get_raster( const std::string &semantic_id, uint64_t entry_id );
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
private:
	std::unique_ptr<GenericRaster> get_raster_from_remote( const GenericOperator &op, const QueryRectangle &rect ) ;
	RasterCache local_cache;
};


#endif /* CACHE_H_ */
