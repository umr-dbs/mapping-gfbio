/*
 * cache_manager.h
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#ifndef MANAGER_H_
#define MANAGER_H_

#include "cache/node/node_cache.h"
#include "cache/priv/shared.h"

#include "operators/operator.h"

#include <unordered_map>
#include <unordered_set>
#include <memory>


/**
 * Interface hiding a single cache and providing
 * application-oriented access to it.
 */
template<typename T>
class CacheWrapper {
public:
	virtual ~CacheWrapper() = default;

	/**
	 * Inserts an item into the cache
	 * @param semantic_id the semantic id
	 * @param item the data-item to cache
	 * @param query the query which produced the result
	 * @param profiler the profiler which recorded the costs of the query-execution
	 * @return whether the entry was stored in the cache or not
	 */
	virtual bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler) = 0;

	/**
	 * Queries for an item satisfying the given request.
	 * The result is a copy of the cached version and may be modified.
	 * @param op the operator-graph of the query
	 * @param rect the query-rectangle
	 * @param profiler the profiler recording costs of query-execution
	 * @return the result satisfying the given query parameters
	 */
	virtual std::unique_ptr<T> query(GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler) = 0;
};

/**
 * The manager gives access to the caches for the different data-types
 */
class CacheManager {
public:
	/**
	 * @return the current instance of the CacheManager
	 */
	static CacheManager& get_instance();
	/**
	 * Initializes the CacheManager with the given implementation
	 */
	static void init( CacheManager *instance );
private:
	static CacheManager *instance;
public:
	virtual ~CacheManager() = default;

	/**
	 * @return the wrapper for the raster-cache
	 */
	virtual CacheWrapper<GenericRaster>& get_raster_cache() = 0;

	/**
	 * @return the wrapper for the raster-cache
	 */
	virtual CacheWrapper<PointCollection>& get_point_cache() = 0;

	/**
	 * @return the wrapper for the line-cache
	 */
	virtual CacheWrapper<LineCollection>& get_line_cache() = 0;

	/**
	 * @return the wrapper for the polygon-cache
	 */
	virtual CacheWrapper<PolygonCollection>& get_polygon_cache() = 0;

	/**
	 * @return the wrapper for the plot-cache
	 */
	virtual CacheWrapper<GenericPlot>& get_plot_cache() = 0;
private:

};


/**
 * This is a NOP-implementation of the CacheWrapper.
 * It always returns a miss on query-methods, so that results
 * must always be computed from scratch.
 */
template<typename T>
class NopCacheWrapper : public CacheWrapper<T> {
public:
	NopCacheWrapper();

	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler);
};

/**
 * This is a NOP-implementation of the CacheManager.
 * May be used on the Node-Server to disable caching or
 * on client-application to omit the index-server and force local computation.
 */
class NopCacheManager : public CacheManager {
public:
	NopCacheManager();
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
private:
	NopCacheWrapper<GenericRaster> raster_cache;
	NopCacheWrapper<PointCollection> point_cache;
	NopCacheWrapper<LineCollection> line_cache;
	NopCacheWrapper<PolygonCollection> poly_cache;
	NopCacheWrapper<GenericPlot> plot_cache;
};

/**
 * This is an implementation of the CacheWrapper which should be used
 * on the client-side to access the cluster.
 * The query-method always delegates requests to the index-server
 * and fetches responses from the corresponding node.
 */
template<typename T>
class ClientCacheWrapper : public CacheWrapper<T> {
public:
	ClientCacheWrapper( CacheType type, const std::string &idx_host, int idx_port );
	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler);
protected:
	std::unique_ptr<T> read_result( BinaryReadBuffer &buffer );
private:
	CacheType type;
	const std::string idx_host;
	const int idx_port;
};

/**
 * This is an implementation of the CacheManager which should be used
 * on the client-side to access the cluster.
 */
class ClientCacheManager : public CacheManager {
public:
	/**
	 * Constructs a new instance
	 * @param idx_host the hostname of the index-server
	 * @param idx_port the port, the index-server listens
	 */
	ClientCacheManager(const std::string &idx_host, int idx_port);
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
private:

	std::string idx_host;
	int idx_port;

	ClientCacheWrapper<GenericRaster> raster_cache;
	ClientCacheWrapper<PointCollection> point_cache;
	ClientCacheWrapper<LineCollection> line_cache;
	ClientCacheWrapper<PolygonCollection> poly_cache;
	ClientCacheWrapper<GenericPlot> plot_cache;
};

#endif /* MANAGER_H_ */
