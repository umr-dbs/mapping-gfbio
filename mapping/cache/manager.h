/*
 * cache_manager.h
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#ifndef MANAGER_H_
#define MANAGER_H_

#include "cache/node/node_cache.h"
#include "cache/priv/caching_strategy.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/transfer.h"

#include <unordered_map>
#include <unordered_set>

//
// The cache-manager provides uniform access to the cache
// Currently used by the node-server process and the getCached*-Methods of GenericOperator
//

class CacheManager {
public:
	// The current instance
	static CacheManager& getInstance();
	// The strategy deciding whether to cache a result or not
	static CachingStrategy& get_strategy();
	// Inititalizes the manager with the given implementation and strategy
	static void init(std::unique_ptr<CacheManager> impl, std::unique_ptr<CachingStrategy> strategy);
	// Index-connection per worker
	static thread_local UnixSocket *remote_connection;

	// Processes the given puzzle-request
	static std::unique_ptr<GenericRaster> process_raster_puzzle(const PuzzleRequest& req, std::string my_host,
				uint32_t my_port);

	virtual ~CacheManager();

	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect) = 0;

	virtual NodeCacheRef put_raster_local(const std::string &semantic_id,
		const std::unique_ptr<GenericRaster> &raster) = 0;

	virtual void remove_raster_local(const NodeCacheKey &key) = 0;
	virtual std::unique_ptr<GenericRaster> get_raster_local(const NodeCacheKey &key) = 0;

	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster) = 0;

	virtual NodeHandshake get_handshake( const std::string &my_host, uint32_t my_port ) const = 0;
	virtual NodeStats get_stats() const = 0;

protected:

	static std::unique_ptr<GenericRaster> fetch_raster(const std::string & host, uint32_t port,
		const NodeCacheKey &key);

	static std::unique_ptr<GenericRaster> do_puzzle(const QueryRectangle &query,
			const geos::geom::Geometry &covered, const std::vector<std::unique_ptr<GenericRaster> >& items);
private:
	static std::unique_ptr<CacheManager> impl;
	static std::unique_ptr<CachingStrategy> strategy;
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
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);

	virtual std::unique_ptr<GenericRaster> get_raster_local(const NodeCacheKey &key);
	virtual NodeCacheRef put_raster_local(const std::string &semantic_id,
		const std::unique_ptr<GenericRaster> &raster);
	virtual void remove_raster_local(const NodeCacheKey &key);

	virtual NodeHandshake get_handshake( const std::string &my_host, uint32_t my_port ) const;
	virtual NodeStats get_stats() const;
private:
	NodeRasterCache rasterCache;
};

//
// Null-Implementation -> used if caching is disabled
//
class NopCacheManager: public CacheManager {
public:
	virtual ~NopCacheManager();
	virtual std::unique_ptr<GenericRaster> query_raster(const GenericOperator &op,
		const QueryRectangle &rect);
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);

	virtual std::unique_ptr<GenericRaster> get_raster_local(const NodeCacheKey &key);
	virtual NodeCacheRef put_raster_local(const std::string &semantic_id,
		const std::unique_ptr<GenericRaster> &raster);
	virtual void remove_raster_local(const NodeCacheKey &key);

	virtual NodeHandshake get_handshake( const std::string &my_host, uint32_t my_port ) const;
	virtual NodeStats get_stats() const;
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
	virtual void put_raster(const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster);

	virtual std::unique_ptr<GenericRaster> get_raster_local(const NodeCacheKey &key);
	virtual NodeCacheRef put_raster_local(const std::string &semantic_id,
		const std::unique_ptr<GenericRaster> &raster);
	virtual void remove_raster_local(const NodeCacheKey &key);

	virtual NodeHandshake get_handshake( const std::string &my_host, uint32_t my_port ) const;
	virtual NodeStats get_stats() const;
private:
	NodeRasterCache local_cache;
	std::string my_host;
	uint32_t my_port;
};

#endif /* MANAGER_H_ */
