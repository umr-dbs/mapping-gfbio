/*
 * util.h
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#ifndef CACHE_NODE_MANAGER_H_
#define CACHE_NODE_MANAGER_H_

#include "cache/node/puzzle_util.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/caching_strategy.h"
#include "cache/priv/connection.h"
#include "cache/manager.h"

#include <string>
#include <memory>


class NodeCacheManager;

/**
 * This class holds the current query-statistics.
 * The modifier methods ensure mutual exclusion,
 */
class ActiveQueryStats : private QueryStats {
public:
	/** Adds a single local hit */
	void add_single_local_hit();

	/** Adds a local hit with multiple cache-entries */
	void add_multi_local_hit();

	/** Adds a partial local hit, with remainders */
	void add_multi_local_partial();

	/** Adds a single remote hit */
	void add_single_remote_hit();

	/** Adds a remote hit with multiple cache-entries */
	void add_multi_remote_hit();

	/** Adds a partial remote hit, with remainders */
	void add_multi_remote_partial();

	/** Adds a full miss */
	void add_miss();

	/**
	 * @return the current counters
	 */
	QueryStats get() const;

	/**
	 * Returns the current counters and resets them to 0
	 * @return the current counters
	 */
	QueryStats get_and_reset();
private:
	mutable std::mutex mtx;
};

/**
 * This class holds all information sensitive for a worker-thread
 */
class WorkerContext {
	friend class PuzzleGuard;
public:
	/** Constructs a new instance */
	WorkerContext();
	WorkerContext( const WorkerContext& ) = delete;
	WorkerContext( WorkerContext&& ) = delete;
	WorkerContext& operator=( const WorkerContext& ) = delete;
	WorkerContext& operator=( WorkerContext&& ) = delete;

	/**
	 * Stores the worker's connection to the index-server
	 * @param con this worker's connection to the index-server
	 */
	void set_index_connection( BlockingConnection *con );

	/**
	 * This function is used to omit caching of puzzle-pieces
	 * @return whether this worker is currently puzzling a result
	 */
	bool is_puzzling() const;

	/**
	 * @return this worker's connection to the index-server
	 */
	BlockingConnection& get_index_connection() const;
private:
	bool puzzling;
	BlockingConnection* index_connection;
};

/**
 * Simple guard to ensure the puzzling flag in this worker's context
 * is released, even if an exception is thrown.
 */
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


/**
 * A cache-wrapper extended with the needs of a cache-node.
 */
template<typename T>
class NodeCacheWrapper : public CacheWrapper<T> {
	friend class NodeServer;
	friend class DeliveryManager;
public:
	/**
	 * Constructs a new instance
	 * @param mgr the parent manager
	 * @param cache the cache to wrap
	 * @param puzzler the puzzler-instance
	 */
	NodeCacheWrapper( NodeCacheManager &mgr, NodeCache<T> &cache,
			std::unique_ptr<Puzzler<T>> puzzler );
	virtual ~NodeCacheWrapper() = default;

	/**
	 * Inserts a result into the cache, if the strategy confirms it. After storing
	 * the item in the local cache, the index-server is called to tell about the entry.
	 * @param semantic_id the semantic id
	 * @param item the data-item to cache
	 * @param query the query which produced the result
	 * @param profiler the profiler which recorded the costs of the query-execution
	 * @return whether the entry was stored in the cache or not
	 */
	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);

	/**
	 * Queries the cache with the given operator and query rectangle. If no local results can be found,
	 * the index-server is called too look up the caches of ther nodes. If no result can be found,
	 * a NoSuchElementException is thrown.
	 * @param op the operator-graph of the query
	 * @param rect the query-rectangle
	 * @param profiler the profiler recording costs of query-execution
	 * @return the result satisfying the given query parameters
	 */
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler);

	/**
	 * Retrieves the item for the given key.
	 * @param key the key of the item to retrieve
	 * @return the entry for the given key
	 */
	std::shared_ptr<const NodeCacheEntry<T>> get(const NodeCacheKey &key);

	/**
	 * Processes the given puzzle-request
	 * @param request the puzzle-request
	 * @param parent_profiler the profiler of the running query
	 * @return the result satisfying the given puzzle-request
	 */
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request, QueryProfiler &parent_profiler );

	QueryStats get_and_reset_query_stats();

private:
	/**
	 * Inserts the given item into the local cache. This operation does not confirm insertion
	 * with the caching-strategy and also does not inform the index about the new entry.
	 * This method is provided for migrating entries from other nodes.
	 * @return the meta-information about the stored entry
	 */
	MetaCacheEntry put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, CacheEntry &&info );

	/**
	 * Removes the element with the given key from the cache, not notifying the index-server about it.
	 * This method sould only be called during reorganization of the system.
	 */
	void remove_local(const NodeCacheKey &key);

private:
	NodeCacheManager &mgr;
	NodeCache<T> &cache;
	std::unique_ptr<RemoteRetriever<T>> retriever;
	std::unique_ptr<PuzzleUtil<T>> puzzle_util;
	RWLock local_lock;
	ActiveQueryStats stats;
};

class NodeCacheManager : public CacheManager, public CacheRefHandler {
	template<class T> friend class NodeCacheWrapper;
	static thread_local WorkerContext context;

public:

	/**
	 * Constructs a new instance
	 * @param strategy the caching strategy to use
	 * @param raster_cache_size the maximum size of the raster cache (in bytes)
	 * @param point_cache_size the maximum size of the point cache (in bytes)
	 * @param line_cache_size the maximum size of the line cache (in bytes)
	 * @param polygon_cache_size the maximum size of the polygon cache (in bytes)
	 * @param plot_cache_size the maximum size of the plot cache (in bytes)
	 */
	NodeCacheManager( std::unique_ptr<CachingStrategy> strategy,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );

	/**
	 * @return the thread-sensitve worker-context
	 */
	WorkerContext &get_worker_context();

	/**
	 * Sets the port of this node's delivery-proccess
	 * @param the port to set
	 */
	void set_self_port(uint32_t port);

	/**
	 * Sets the hostname, this node is referenced with
	 * @param host the hostname to set
	 */
	void set_self_host( const std::string &host );

	/**
	 * Creates a self-reference to the cache-entry with the given id
	 * @param id the id of the cache-entry to reference
	 * @return a reference to the cache-entry with the given id
	 */
	CacheRef create_local_ref(uint64_t id) const;

	/**
	 * Checks whether the given reference points to this node
	 * @param ref the reference to check
	 * @return whether the given reference points to this node
	 */
	bool is_local_ref(const CacheRef& ref) const;

	/**
	 * Creates the handshake to be sent to the index-server
	 * when creating the initial control-connection.
	 * @return the handshake-data
	 */
	NodeHandshake create_handshake() const;

	/**
	 * Retrieves the statistics delta since the last call to this method.
	 * @return the statistics
	 */
	NodeStats get_stats_delta() const;

	/**
	 * Retrieves the cumulated query-stats for this node.
	 * This is NOT a delta.
	 * @return the cumulated query-stats for this node
	 */
	QueryStats get_cumulated_query_stats() const;

	/**
	 * @return the wrapper for the raster-cache
	 */
	NodeCacheWrapper<GenericRaster>& get_raster_cache();

	/**
	 * @return the wrapper for the point-cache
	 */
	NodeCacheWrapper<PointCollection>& get_point_cache();

	/**
	 * @return the wrapper for the line-cache
	 */
	NodeCacheWrapper<LineCollection>& get_line_cache();

	/**
	 * @return the wrapper for the polygon-cache
	 */
	NodeCacheWrapper<PolygonCollection>& get_polygon_cache();

	/**
	 * @return the wrapper for the plot-cache
	 */
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
