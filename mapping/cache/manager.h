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
#include "datatypes/raster.h"

#include <unordered_map>
#include <unordered_set>
#include <memory>

//
// The cache-manager provides uniform access to the cache
// Currently used by the node-server process and the getCached*-Methods of GenericOperator
//
template<typename T>
class CacheWrapper {
public:
	virtual ~CacheWrapper() = default;

	// Inserts an item into the cache
	virtual void put(const std::string &semantic_id, const std::unique_ptr<T> &item) = 0;

	// Queries for an item satisfying the given request
	// The result is a copy of the cached version and may be modified
	virtual std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect) = 0;

	// Inserts an element into the local cache -- omitting any communication
	// to the remote server
	virtual NodeCacheRef put_local(const std::string &semantic_id, const std::unique_ptr<T> &raster, const AccessInfo info = AccessInfo() ) = 0;

	// Removes the element with the given key from the cache,
	// not notifying the index (if applicable)
	virtual void remove_local(const NodeCacheKey &key) = 0;

	// Gets a reference to the cached element for the given key
	// The result is not a copy and may only be used for delivery purposes
	virtual const std::shared_ptr<const T> get_ref(const NodeCacheKey &key) = 0;

	virtual NodeCacheRef get_entry_info( const NodeCacheKey &key) = 0;

	virtual std::unique_ptr<T> process_puzzle( const PuzzleRequest& request ) = 0;
};

template<typename T>
class NopCacheWrapper : public CacheWrapper<T> {
public:
	NopCacheWrapper();
	void put(const std::string &semantic_id, const std::unique_ptr<T> &item);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
	NodeCacheRef put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, const AccessInfo info = AccessInfo() );
	void remove_local(const NodeCacheKey &key);
	const std::shared_ptr<const T> get_ref(const NodeCacheKey &key);
	NodeCacheRef get_entry_info( const NodeCacheKey &key);
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request );
};



template<typename T>
class RemoteCacheWrapper : public CacheWrapper<T> {
public:
	RemoteCacheWrapper( NodeCache<T> &cache, const std::string &my_host, int my_port );
	virtual ~RemoteCacheWrapper() = default;

	void put(const std::string &semantic_id, const std::unique_ptr<T> &item);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
	NodeCacheRef put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, const AccessInfo info = AccessInfo() );
	void remove_local(const NodeCacheKey &key);
	const std::shared_ptr<const T> get_ref(const NodeCacheKey &key);
	NodeCacheRef get_entry_info( const NodeCacheKey &key);
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request );
protected:
	virtual std::unique_ptr<T> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>>& items) = 0;
	virtual std::unique_ptr<T> read_item( BinaryStream &stream ) = 0;
	virtual std::unique_ptr<T> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) = 0;
private:
	std::unique_ptr<T> fetch_item( const std::string &semantic_id, const CacheRef &ref );
	SpatioTemporalReference enlarge_puzzle( const QueryRectangle &query, const std::vector<std::shared_ptr<const T>>& items);
	std::vector<std::unique_ptr<T>> compute_remainders( const std::string &semantic_id, const T& ref_result, const PuzzleRequest &request );
	NodeCache<T> &cache;
	std::string my_host;
	uint32_t my_port;
};

class RasterCacheWrapper : public RemoteCacheWrapper<GenericRaster> {
public:
	RasterCacheWrapper( NodeCache<GenericRaster> &cache, const std::string &my_host, int my_port );

	std::unique_ptr<GenericRaster> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericRaster>>& items);
	std::unique_ptr<GenericRaster> read_item( BinaryStream &stream );
	std::unique_ptr<GenericRaster> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
};

class PlotCacheWrapper : public RemoteCacheWrapper<GenericPlot> {
public:
	PlotCacheWrapper( NodeCache<GenericPlot> &cache, const std::string &my_host, int my_port );
	std::unique_ptr<GenericPlot> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericPlot>>& items);
	std::unique_ptr<GenericPlot> read_item( BinaryStream &stream );
	std::unique_ptr<GenericPlot> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
};

template<typename T>
class FeatureCollectionCacheWrapper : public RemoteCacheWrapper<T> {
public:
	FeatureCollectionCacheWrapper( NodeCache<T> &cache, const std::string &my_host, int my_port );
	virtual ~FeatureCollectionCacheWrapper<T>() = default;
	std::unique_ptr<T> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>>& items);
	std::unique_ptr<T> read_item( BinaryStream &stream );
	virtual std::unique_ptr<T> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) = 0;
	virtual void append_idxs( T &dest, const T &src ) = 0;
protected:
	void append_idx_vec( std::vector<uint32_t> &dest, const std::vector<uint32_t> &src );
private:
	void combine_feature_attributes( AttributeArrays &dest, const AttributeArrays src );
};

class PointCollectionCacheWrapper : public FeatureCollectionCacheWrapper<PointCollection> {
public:
	PointCollectionCacheWrapper( NodeCache<PointCollection> &cache, const std::string &my_host, int my_port );
	std::unique_ptr<PointCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( PointCollection &dest, const PointCollection &src );
};

class LineCollectionCacheWrapper : public FeatureCollectionCacheWrapper<LineCollection> {
public:
	LineCollectionCacheWrapper( NodeCache<LineCollection> &cache, const std::string &my_host, int my_port );
	std::unique_ptr<LineCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( LineCollection &dest, const LineCollection &src );
};

class PolygonCollectionCacheWrapper : public FeatureCollectionCacheWrapper<PolygonCollection> {
public:
	PolygonCollectionCacheWrapper( NodeCache<PolygonCollection> &cache, const std::string &my_host, int my_port );
	std::unique_ptr<PolygonCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( PolygonCollection &dest, const PolygonCollection &src );
};



class CacheManager {
public:
	// The strategy deciding whether to cache a result or not
	static CachingStrategy& get_strategy();

	static CacheManager& get_instance();

	// Inititalizes the manager with the given implementation and strategy
	static void init( std::unique_ptr<CacheManager> instance,
			std::unique_ptr<CachingStrategy> strategy );

	// Index-connection -- set per worker DO NOT TOUCH
	static thread_local UnixSocket *remote_connection;

	//
	// INSTANCE METHODS
	//

	virtual ~CacheManager() = default;

	// Creates a handshake message for the index-server
	virtual NodeHandshake get_handshake() const = 0;

	// Retrieves statistics for this cache
	virtual NodeStats get_stats() const = 0;

	virtual CacheWrapper<GenericRaster>& get_raster_cache() = 0;
	virtual CacheWrapper<PointCollection>& get_point_cache() = 0;
	virtual CacheWrapper<LineCollection>& get_line_cache() = 0;
	virtual CacheWrapper<PolygonCollection>& get_polygon_cache() = 0;
	virtual CacheWrapper<GenericPlot>& get_plot_cache() = 0;

private:
	// Holds the actual caching-strategy to use
	static std::unique_ptr<CachingStrategy> strategy;
	static std::unique_ptr<CacheManager> instance;
};

class DefaultCacheManager : public CacheManager {
public:
	DefaultCacheManager( const std::string &my_host, int my_port,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );

	// Creates a handshake message for the index-server
	NodeHandshake get_handshake() const;

	// Retrieves statistics for this cache
	NodeStats get_stats() const;

	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();

private:
	std::string my_host;
	int my_port;

	NodeRasterCache raster_cache;
	NodePointCache point_cache;
	NodeLineCache line_cache;
	NodePolygonCache polygon_cache;
	NodePlotCache plot_cache;

	RasterCacheWrapper raster_wrapper;
	PointCollectionCacheWrapper point_wrapper;
	LineCollectionCacheWrapper line_wrapper;
	PolygonCollectionCacheWrapper polygon_wrapper;
	PlotCacheWrapper plot_wrapper;
};

class NopCacheManager : public CacheManager {
public:
	NopCacheManager(const std::string &my_host, int my_port);

	// Creates a handshake message for the index-server
	NodeHandshake get_handshake() const;

	// Retrieves statistics for this cache
	NodeStats get_stats() const;

	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
private:
	std::string my_host;
	int my_port;

	NopCacheWrapper<GenericRaster> raster_cache;
	NopCacheWrapper<PointCollection> point_cache;
	NopCacheWrapper<LineCollection> line_cache;
	NopCacheWrapper<PolygonCollection> poly_cache;
	NopCacheWrapper<GenericPlot> plot_cache;
};

#endif /* MANAGER_H_ */
