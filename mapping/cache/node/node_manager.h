/*
 * util.h
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#ifndef CACHE_NODE_MANAGER_H_
#define CACHE_NODE_MANAGER_H_

#include "util/binarystream.h"
#include "cache/priv/transfer.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/caching_strategy.h"
#include "cache/manager.h"

#include <string>
#include <memory>


class NodeCacheManager;

//
// Node-Cache
// Gives uniform access to the real cache-implementation
// on the nodes
//

template<typename T>
class NodeCacheWrapper : public CacheWrapper<T> {
	friend class NodeCacheManager;
public:
	NodeCacheWrapper( const NodeCacheManager &mgr, NodeCache<T> &cache, const CachingStrategy &strategy );
	virtual ~NodeCacheWrapper() = default;

	void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);

	// Inserts an element into the local cache -- omitting any communication
	// to the remote server
	NodeCacheRef put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, CacheEntry &&info );

	// Removes the element with the given key from the cache,
	// not notifying the index (if applicable)
	void remove_local(const NodeCacheKey &key);

	// Gets a reference to the cached element for the given key
	// The result is not a copy and may only be used for delivery purposes
	const std::shared_ptr<const T> get_ref(const NodeCacheKey &key);

	// Returns only meta-information about the entry for the given key
	NodeCacheRef get_entry_info( const NodeCacheKey &key);

	// Proccesses the given puzzle-request
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request, QueryProfiler &profiler );
protected:
	virtual std::unique_ptr<T> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>>& items) = 0;
	virtual std::unique_ptr<T> read_item( BinaryStream &stream ) = 0;
	virtual std::unique_ptr<T> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) = 0;
private:
	std::unique_ptr<T> fetch_item( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp );
	SpatioTemporalReference enlarge_puzzle( const QueryRectangle &query, const std::vector<std::shared_ptr<const T>>& items);
	std::vector<std::unique_ptr<T>> compute_remainders( const std::string &semantic_id, const T& ref_result, const PuzzleRequest &request, QueryProfiler &profiler );
	const NodeCacheManager &mgr;
	NodeCache<T> &cache;
	const CachingStrategy &strategy;
	RWLock local_lock;
	QueryStats stats;
};

class RasterCacheWrapper : public NodeCacheWrapper<GenericRaster> {
public:
	RasterCacheWrapper( const NodeCacheManager &mgr, NodeCache<GenericRaster> &cache, const CachingStrategy &strategy );

	std::unique_ptr<GenericRaster> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericRaster>>& items);
	std::unique_ptr<GenericRaster> read_item( BinaryStream &stream );
	std::unique_ptr<GenericRaster> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
};

class PlotCacheWrapper : public NodeCacheWrapper<GenericPlot> {
public:
	PlotCacheWrapper( const NodeCacheManager &mgr, NodeCache<GenericPlot> &cache, const CachingStrategy &strategy );
	std::unique_ptr<GenericPlot> do_puzzle(const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericPlot>>& items);
	std::unique_ptr<GenericPlot> read_item( BinaryStream &stream );
	std::unique_ptr<GenericPlot> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
};

template<typename T>
class FeatureCollectionCacheWrapper : public NodeCacheWrapper<T> {
public:
	FeatureCollectionCacheWrapper( const NodeCacheManager &mgr, NodeCache<T> &cache, const CachingStrategy &strategy );
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
	PointCollectionCacheWrapper( const NodeCacheManager &mgr, NodeCache<PointCollection> &cache, const CachingStrategy &strategy );
	std::unique_ptr<PointCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( PointCollection &dest, const PointCollection &src );
};

class LineCollectionCacheWrapper : public FeatureCollectionCacheWrapper<LineCollection> {
public:
	LineCollectionCacheWrapper( const NodeCacheManager &mgr, NodeCache<LineCollection> &cache, const CachingStrategy &strategy );
	std::unique_ptr<LineCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( LineCollection &dest, const LineCollection &src );
};

class PolygonCollectionCacheWrapper : public FeatureCollectionCacheWrapper<PolygonCollection> {
public:
	PolygonCollectionCacheWrapper( const NodeCacheManager &mgr, NodeCache<PolygonCollection> &cache, const CachingStrategy &strategy );
	std::unique_ptr<PolygonCollection> compute_item ( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp );
	void append_idxs( PolygonCollection &dest, const PolygonCollection &src );
};


class NodeCacheManager : public CacheManager {
private:
	static thread_local BinaryStream *index_connection;

public:
	NodeCacheManager( std::unique_ptr<CachingStrategy> strategy,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );

	void set_index_connection( BinaryStream *con );
	BinaryStream &get_index_connection() const;

	void set_self_port(uint32_t port);
	void set_self_host( const std::string &host );

	CacheRef create_self_ref(uint64_t id) const;
	bool is_self_ref(const CacheRef& ref) const;

	NodeHandshake create_handshake() const;
	NodeStats get_stats() const;

	const QueryStats& get_query_stats() const;

	void reset_query_stats();

	NodeCacheWrapper<GenericRaster>& get_raster_cache();
	NodeCacheWrapper<PointCollection>& get_point_cache();
	NodeCacheWrapper<LineCollection>& get_line_cache();
	NodeCacheWrapper<PolygonCollection>& get_polygon_cache();
	NodeCacheWrapper<GenericPlot>& get_plot_cache();
private:
	NodeCache<GenericRaster> raster_cache;
	NodeCache<PointCollection> point_cache;
	NodeCache<LineCollection> line_cache;
	NodeCache<PolygonCollection> polygon_cache;
	NodeCache<GenericPlot> plot_cache;

	mutable RasterCacheWrapper raster_wrapper;
	mutable PointCollectionCacheWrapper point_wrapper;
	mutable LineCollectionCacheWrapper line_wrapper;
	mutable PolygonCollectionCacheWrapper polygon_wrapper;
	mutable PlotCacheWrapper plot_wrapper;

	// Holds the actual caching-strategy to use
	std::unique_ptr<CachingStrategy> strategy;

	std::string my_host;
	uint32_t my_port;

	mutable QueryStats cumulated_stats;
};

#endif /* CACHE_NODE_MANAGER_H_ */
