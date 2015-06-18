/*
 * cache_manager.h
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#ifndef MANAGER_H_
#define MANAGER_H_

#include "cache/cache.h"

//
// The cache-manager provides uniform access to the cache
// Currently used by the node-server process and the getCached*-Methods of GenericOperator
//

class CacheManager {
public:
	static CacheManager& getInstance();
	static void init(std::unique_ptr<CacheManager> &impl);
	static thread_local SocketConnection *remote_connection;

	virtual ~CacheManager();

	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster) = 0;
	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect) = 0;
	virtual std::unique_ptr<GenericRaster> get_raster(const std::string &semantic_id, uint64_t entry_id) = 0;

	std::unique_ptr<GenericRaster> get_raster(const STCacheKey &key);

	static std::unique_ptr<GenericRaster> do_puzzle(const QueryRectangle &query,
		const geos::geom::Geometry &covered, const std::vector<std::unique_ptr<GenericRaster> >& items);
protected:
	//
	// Fetches a raster directly from the cache, omitting the operator-graph.
	//
	static std::unique_ptr<GenericRaster> fetch_raster(const std::string & host, uint32_t port,
		const STCacheKey &key);
private:
	static std::unique_ptr<CacheManager> impl;
};


//
// Implementation using only the local cache
//
class LocalCacheManager: public CacheManager {
public:
	LocalCacheManager(size_t rasterCacheSize);
	virtual ~LocalCacheManager();
	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect);
	virtual std::unique_ptr<GenericRaster> get_raster(const std::string &semantic_id, uint64_t entry_id);
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);
private:
	RasterCache rasterCache;
};

//
// Null-Implementation -> used if caching is disabled
//
class NopCacheManager: public CacheManager {
public:
	virtual ~NopCacheManager();
	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect);
	virtual std::unique_ptr<GenericRaster> get_raster(const std::string &semantic_id, uint64_t entry_id);
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);
};


//
// Hybrid implementation. Always looks up the local-cache
// first, before asking the index-server.
// To be used in cache-nodes
//
class RemoteCacheManager: public CacheManager {
public:
	RemoteCacheManager(size_t rasterCacheSize, const std::string &my_host, uint32_t my_port);
	virtual ~RemoteCacheManager();
	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect);
	virtual std::unique_ptr<GenericRaster> get_raster(const std::string &semantic_id, uint64_t entry_id);
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);
private:
	RasterCache local_cache;
	std::string my_host;
	uint32_t my_port;
};

#endif /* MANAGER_H_ */
