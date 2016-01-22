/*
 * util.h
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#ifndef CACHE_NODE_MANAGER_H_
#define CACHE_NODE_MANAGER_H_

#include "util/binarystream.h"
#include "cache/node/puzzle_util.h"
#include "cache/priv/transfer.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/caching_strategy.h"
#include "cache/manager.h"

#include <string>
#include <memory>


class NodeCacheManager;

class WorkerContext {
	friend class PuzzleGuard;
public:
	WorkerContext();
	void set_index_connection( BinaryStream *stream );
	bool is_puzzling() const;
	BinaryStream& get_index_connection() const;
private:
	bool puzzling;
	BinaryStream* index_connection;
};

class PuzzleGuard {
public:
	PuzzleGuard( WorkerContext& ctx ) : ctx(ctx) { ctx.puzzling = true; };
	~PuzzleGuard() { ctx.puzzling = false; };
private:
	WorkerContext& ctx;
};


//
// Node-Cache
// Gives uniform access to the real cache-implementation
// on the nodes
//

template<typename T>
class NodeCacheWrapper : public CacheWrapper<T> {
	friend class NodeCacheManager;
public:
	NodeCacheWrapper( NodeCacheManager &mgr, NodeCache<T> &cache,
			std::unique_ptr<RemoteRetriever<T>> retriever,
			std::unique_ptr<Puzzler<T>> puzzler,
			const CachingStrategy &strategy );
	virtual ~NodeCacheWrapper() = default;

	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler);

	// Inserts an element into the local cache -- omitting any communication
	// to the remote server
	NodeCacheRef put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, CacheEntry &&info );

	// Removes the element with the given key from the cache,
	// not notifying the index (if applicable)
	void remove_local(const NodeCacheKey &key);

	std::shared_ptr<const NodeCacheEntry<T>> get(const NodeCacheKey &key);

//	// Gets a reference to the cached element for the given key
//	// The result is not a copy and may only be used for delivery purposes
//	const std::shared_ptr<const T> get_ref(const NodeCacheKey &key);
//
//	// Returns only meta-information about the entry for the given key
//	NodeCacheRef get_entry_info( const NodeCacheKey &key);

	// Proccesses the given puzzle-request
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request, QueryProfiler &parent_profiler );
private:
	NodeCacheManager &mgr;
	NodeCache<T> &cache;
	std::unique_ptr<RemoteRetriever<T>> retriever;
	std::unique_ptr<PuzzleUtil<T>> puzzle_util;
	const CachingStrategy &strategy;
	RWLock local_lock;
	QueryStats stats;
};

class NodeCacheManager : public CacheManager, public CacheRefHandler {
private:
	static thread_local WorkerContext context;
public:
	NodeCacheManager( std::unique_ptr<CachingStrategy> strategy,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );

	WorkerContext &get_worker_context();

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

	mutable NodeCacheWrapper<GenericRaster> raster_wrapper;
	mutable NodeCacheWrapper<PointCollection> point_wrapper;
	mutable NodeCacheWrapper<LineCollection> line_wrapper;
	mutable NodeCacheWrapper<PolygonCollection> polygon_wrapper;
	mutable NodeCacheWrapper<GenericPlot> plot_wrapper;

	// Holds the actual caching-strategy to use
	std::unique_ptr<CachingStrategy> strategy;

	std::string my_host;
	uint32_t my_port;

	mutable QueryStats cumulated_stats;
};

#endif /* CACHE_NODE_MANAGER_H_ */
