/*
 * local_cache.h
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

#ifndef CACHE_H_
#define CACHE_H_

#include <string>
#include <memory>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>
#include "cache/replacementpolicy.h"
#include "util/log.h"
#include "datatypes/raster.h"

class SocketConnection;


template<typename EType>
class STCacheStructure;


std::string qrToString( const QueryRectangle &rect );

std::string strefToString( const SpatioTemporalReference &ref );


//
// Represents a single entry in the cache
//
template<typename EType>
class STCacheEntry {
public:
	STCacheEntry( std::unique_ptr<EType> result,
				  size_t size,
				  STCacheStructure<EType> *structure ) :
					  size(sizeof(STCacheEntry) + size), structure(structure), result(std::move(result) ) {};
	~STCacheEntry() {
		Log::debug("STCacheEntry destroyed");
	}
	std::unique_ptr<EType> result;
	size_t size;
	STCacheStructure<EType> *structure;
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
	virtual void insert( const std::shared_ptr<STCacheEntry<EType>> &entry ) = 0;
	virtual std::shared_ptr<STCacheEntry<EType>> query( const QueryRectangle &spec ) = 0;
	virtual void remove( const std::shared_ptr<STCacheEntry<EType>> &entry ) = 0;
};

//
// Abstract cache class. To be completed per Entry-Type
//
template<typename EType>
class STCache {
public:
	STCache( size_t max_size ) : max_size(max_size), current_size(0) {
		Log::debug("Creating new cache with max-size: %d", max_size);
		policy = std::unique_ptr<ReplacementPolicy<EType>>( new LRUPolicy<EType>() );
	};
	virtual ~STCache();
	void   put( const std::string &key, const std::unique_ptr<EType> &item);
	std::unique_ptr<EType> get( const std::string &key, const QueryRectangle &qr );
protected:
	virtual size_t getContentSize( const std::unique_ptr<EType> &content )= 0;
	virtual std::unique_ptr<EType> copyContent( const std::unique_ptr<EType> &content ) = 0;
	virtual STCacheStructure<EType>* newStructure() = 0;
private:
	STCacheStructure<EType>* getStructure( const std::string &key, bool create = false );
	std::shared_ptr<STCacheEntry<EType>> newEntry( STCacheStructure<EType> *structure, const std::unique_ptr<EType> &result );
	size_t max_size;
	size_t current_size;
	std::unique_ptr<ReplacementPolicy<EType>> policy;
	std::unordered_map<std::string,STCacheStructure<EType>*> caches;
	std::mutex mtx;


};

//
// Raster Cache
//

class RasterCache : public STCache<GenericRaster> {
public:
	RasterCache() = delete;
	RasterCache( size_t size ) : STCache(size) {};
protected:
	virtual size_t getContentSize( const std::unique_ptr<GenericRaster> &content );
	virtual std::unique_ptr<GenericRaster> copyContent( const std::unique_ptr<GenericRaster> &content );
	virtual STCacheStructure<GenericRaster>* newStructure();
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
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect ) = 0;
	virtual void putRaster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster ) = 0;
private:
	static std::unique_ptr<CacheManager> impl;
};

class LocalCacheManager : public CacheManager {
public:
	LocalCacheManager( size_t rasterCacheSize ) : rasterCache(rasterCacheSize) {};
	virtual ~LocalCacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect ) ;
	virtual void putRaster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
private:
	RasterCache rasterCache;
};

class NopCacheManager : public CacheManager {
public:
	virtual ~NopCacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect ) ;
	virtual void putRaster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
};

class RemoteCacheManager : public CacheManager {
public:
	virtual ~RemoteCacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect ) ;
	virtual void putRaster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
};

class HybridCacheManager : public RemoteCacheManager {
public:
	HybridCacheManager( size_t rasterCacheSize ) : local_cache(rasterCacheSize) {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect ) ;
	virtual void putRaster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster );
private:
	LocalCacheManager local_cache;
};


#endif /* CACHE_H_ */
