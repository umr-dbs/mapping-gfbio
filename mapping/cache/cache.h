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
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"
#include "cache/replacementpolicy.h"
#include "cache/log.h"

class QueryRectangle;

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
		Log::log(DEBUG, "STCacheEntry destroyed");
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
// A simple std::vector based implementation of the
// STCacheStructure interface
//
template<typename EType>
class STListCacheStructure : public STCacheStructure<EType> {
public:
	virtual ~STListCacheStructure() {};
	virtual void insert( const std::shared_ptr<STCacheEntry<EType>> &entry );
	virtual std::shared_ptr<STCacheEntry<EType>> query( const QueryRectangle &spec );
	virtual void remove( const std::shared_ptr<STCacheEntry<EType>> &entry );
protected:
	virtual bool matches( const QueryRectangle &spec, const std::shared_ptr<STCacheEntry<EType>> &entry ) const = 0;
private:
	std::vector<std::shared_ptr<STCacheEntry<EType>>> entries;
};

class STRasterCacheStructure : public STListCacheStructure<GenericRaster> {
public:
	virtual ~STRasterCacheStructure() {};
protected:
	virtual bool matches( const QueryRectangle &spec, const std::shared_ptr<STCacheEntry<GenericRaster>> &entry ) const;
};

//
// Virtual class encapsulating result-generation
// on cache misses.
//
template<typename EType>
class Producer {
public:
	virtual ~Producer() {};
	virtual std::unique_ptr<EType> create() const = 0;
};

//
// Abstract cache class. To be completed per Entry-Type
//
template<typename EType>
class STCache {
public:
	STCache( size_t max_size ) : max_size(max_size), current_size(0) {
		Log::log( DEBUG, "Creating new cache with max-size: %d", max_size);
		policy = std::unique_ptr<ReplacementPolicy<EType>>( new LRUPolicy<EType>() );
	};
	virtual ~STCache();
	void   put( const std::string &key, const std::unique_ptr<EType> &item);
	std::unique_ptr<EType> get( const std::string &key, const QueryRectangle &qr );
	std::unique_ptr<EType> getOrCreate( const std::string &key,
			const QueryRectangle &qr, const Producer<EType> &producer );
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
	std::recursive_mutex mtx;


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
	virtual ~CacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect, const Producer<GenericRaster> &producer ) = 0;
private:
	static std::unique_ptr<CacheManager> impl;
};

class DefaultCacheManager : public CacheManager {
public:
	DefaultCacheManager( size_t rasterCacheSize ) : rasterCache(rasterCacheSize) {};
	virtual ~DefaultCacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect, const Producer<GenericRaster> &producer ) ;
private:
	RasterCache rasterCache;
};

class NopCacheManager : public CacheManager {
	virtual ~NopCacheManager() {};
	virtual std::unique_ptr<GenericRaster> getRaster( const std::string &semantic_id, const QueryRectangle &rect, const Producer<GenericRaster> &producer ) ;
};


#endif /* CACHE_H_ */
