/*
 * cache_manager.h
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#ifndef MANAGER_H_
#define MANAGER_H_

#include "cache/node/node_cache.h"
#include "operators/operator.h"
#include "cache/common.h"


#include <unordered_map>
#include <unordered_set>
#include <memory>


//
// The cache-wrapper hides the implementation per type
//
template<typename T>
class CacheWrapper {
public:
	virtual ~CacheWrapper() = default;

	// Inserts an item into the cache
	virtual void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler) = 0;

	// Queries for an item satisfying the given request
	// The result is a copy of the cached version and may be modified
	virtual std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect) = 0;
};

//
// The cache-manager provides uniform access to the cache
//
class CacheManager {
public:
	static CacheManager& get_instance();
	// Inititalizes the manager with the given implementation
	static void init( CacheManager *instance );
private:
	static CacheManager *instance;
public:
	//
	// INSTANCE METHODS
	//
	virtual ~CacheManager() = default;
	virtual CacheWrapper<GenericRaster>& get_raster_cache() = 0;
	virtual CacheWrapper<PointCollection>& get_point_cache() = 0;
	virtual CacheWrapper<LineCollection>& get_line_cache() = 0;
	virtual CacheWrapper<PolygonCollection>& get_polygon_cache() = 0;
	virtual CacheWrapper<GenericPlot>& get_plot_cache() = 0;
private:

};


//
// NOP-Wrapper
// Usage:
// On nodes to disable caching
// On CGI to disable access to the index-server and compute results by itself (old behaviour)
//

template<typename T, CacheType CType>
class NopCacheWrapper : public CacheWrapper<T> {
public:
	NopCacheWrapper();
	void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
};

class NopCacheManager : public CacheManager {
public:
	NopCacheManager();
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
private:
	NopCacheWrapper<GenericRaster,CacheType::RASTER> raster_cache;
	NopCacheWrapper<PointCollection,CacheType::POINT> point_cache;
	NopCacheWrapper<LineCollection,CacheType::LINE> line_cache;
	NopCacheWrapper<PolygonCollection,CacheType::POLYGON> poly_cache;
	NopCacheWrapper<GenericPlot,CacheType::PLOT> plot_cache;
};




//
// Client-Wrapper
// Usage:
// On CGI. So every call to getCached* on operators will be directly sent to the index-server
// and result in a hit.
//

template<typename T, CacheType CType>
class ClientCacheWrapper : public CacheWrapper<T> {
public:
	ClientCacheWrapper( CacheType type, const std::string &idx_host, int idx_port );
	void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
protected:
	std::unique_ptr<T> read_result( BinaryStream &stream );
private:
	const CacheType type;
	const std::string idx_host;
	const int idx_port;
};

class ClientCacheManager : public CacheManager {
public:
	ClientCacheManager(const std::string &idx_host, int idx_port);
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
private:
	std::string idx_host;
	int idx_port;

	ClientCacheWrapper<GenericRaster,CacheType::RASTER> raster_cache;
	ClientCacheWrapper<PointCollection,CacheType::POINT> point_cache;
	ClientCacheWrapper<LineCollection,CacheType::LINE> line_cache;
	ClientCacheWrapper<PolygonCollection,CacheType::POLYGON> poly_cache;
	ClientCacheWrapper<GenericPlot,CacheType::PLOT> plot_cache;
};

#endif /* MANAGER_H_ */
