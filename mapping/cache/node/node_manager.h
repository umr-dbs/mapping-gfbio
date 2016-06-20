/*
 * util.h
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#ifndef CACHE_NODE_MANAGER_H_
#define CACHE_NODE_MANAGER_H_

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

	/** Adds the given amount of bytes */
	void add_result_bytes(uint64_t bytes);

	void add_lost_put();

	/** Adds a full miss */
	void add_miss();

	void add_query(double ratio);

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
	 * @return this worker's connection to the index-server
	 */
	BlockingConnection& get_index_connection() const;
private:
	BlockingConnection* index_connection;
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
	friend class NodeCacheManager;
public:
	/**
	 * Constructs a new instance
	 * @param mgr the parent manager
	 * @param cache the cache to wrap
	 * @param puzzler the puzzler-instance
	 */
	NodeCacheWrapper( NodeCacheManager &mgr, size_t size, CacheType type );
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
	virtual bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler) = 0;

	/**
	 * Queries the cache with the given operator and query rectangle. If no local results can be found,
	 * the index-server is called too look up the caches of ther nodes. If no result can be found,
	 * a NoSuchElementException is thrown.
	 * @param op the operator-graph of the query
	 * @param rect the query-rectangle
	 * @param profiler the profiler recording costs of query-execution
	 * @return the result satisfying the given query parameters
	 */
	virtual std::unique_ptr<T> query(GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler) = 0;

	/**
	 * Inserts the given item into the local cache. This operation does not confirm insertion
	 * with the caching-strategy and also does not inform the index about the new entry.
	 * This method is provided for migrating entries from other nodes.
	 * @return the meta-information about the stored entry
	 */
	virtual MetaCacheEntry put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, CacheEntry &&info ) = 0;

	/**
	 * Removes the element with the given key from the cache, not notifying the index-server about it.
	 * This method sould only be called during reorganization of the system.
	 */
	virtual void remove_local(const NodeCacheKey &key) = 0;

	/**
	 * Processes the given puzzle-request
	 * @param request the puzzle-request
	 * @return the result satisfying the given puzzle-request
	 */
	virtual std::unique_ptr<T> process_puzzle( const PuzzleRequest& request, QueryProfiler &parent_profiler ) = 0;

	/**
	 * Retrieves the item for the given key.
	 * @param key the key of the item to retrieve
	 * @return the entry for the given key
	 */
	std::shared_ptr<const NodeCacheEntry<T>> get(const NodeCacheKey &key) const;

	QueryStats get_and_reset_query_stats();

	CacheType get_type() const;

protected:
	NodeCacheManager &mgr;
	NodeCache<T> cache;
	ActiveQueryStats stats;
};

class NodeCacheManager : public CacheManager {
	static thread_local WorkerContext context;
public:
	static std::unique_ptr<NodeCacheManager> by_name( const std::string &name, size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size, const std::string &strategy = "uncached", const std::string &local_replacement = "lru" );

	/**
	 * Constructs a new instance
	 * @param strategy the caching strategy to use
	 * @param raster_cache_size the maximum size of the raster cache (in bytes)
	 * @param point_cache_size the maximum size of the point cache (in bytes)
	 * @param line_cache_size the maximum size of the line cache (in bytes)
	 * @param polygon_cache_size the maximum size of the polygon cache (in bytes)
	 * @param plot_cache_size the maximum size of the plot cache (in bytes)
	 */
	NodeCacheManager( const std::string &strategy,
			std::unique_ptr<NodeCacheWrapper<GenericRaster>> raster_wrapper,
			std::unique_ptr<NodeCacheWrapper<PointCollection>> point_wrapper,
			std::unique_ptr<NodeCacheWrapper<LineCollection>> line_wrapper,
			std::unique_ptr<NodeCacheWrapper<PolygonCollection>> polygon_wrapper,
			std::unique_ptr<NodeCacheWrapper<GenericPlot>> plot_wrapper );

	/**
	 * @return the thread-sensitve worker-context
	 */
	WorkerContext &get_worker_context();

	const CachingStrategy &get_strategy() const;

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
	mutable std::unique_ptr<NodeCacheWrapper<GenericRaster>> raster_wrapper;
	mutable std::unique_ptr<NodeCacheWrapper<PointCollection>> point_wrapper;
	mutable std::unique_ptr<NodeCacheWrapper<LineCollection>> line_wrapper;
	mutable std::unique_ptr<NodeCacheWrapper<PolygonCollection>> polygon_wrapper;
	mutable std::unique_ptr<NodeCacheWrapper<GenericPlot>> plot_wrapper;

	// Holds the actual caching-strategy to use
	std::unique_ptr<CachingStrategy> strategy;

	mutable QueryStats cumulated_stats;

protected:
	std::string my_host;
	uint32_t my_port;


};

#endif /* CACHE_NODE_MANAGER_H_ */
