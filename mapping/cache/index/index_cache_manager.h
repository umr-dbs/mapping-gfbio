/*
 * index_cache_manager.h
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#ifndef INDEX_INDEX_CACHE_MANAGER_H_
#define INDEX_INDEX_CACHE_MANAGER_H_

#include "cache/index/index_cache.h"
#include "cache/index/index_config.h"
#include "cache/index/reorg_strategy.h"
#include "cache/index/node.h"
#include "cache/priv/cache_stats.h"

#include <string>
#include <memory>



/**
 * Manages all available caches
 */
class IndexCacheManager {
private:
	/**
	 * Combines a cache-instance with an instance of the configured
	 * reorganization strategy
	 */
	class CacheInfo {
	public:
		CacheInfo( CacheType type, const std::string &reorg_strategy, const std::string &relevance_function );
		std::unique_ptr<IndexCache> cache;
		std::unique_ptr<ReorgStrategy> reorg_strategy;
	};
public:
	/**
	 * Manages cache-instances for all data-types
	 */
	IndexCacheManager( const IndexConfig &config );
	IndexCacheManager() = delete;
	IndexCacheManager( const IndexCacheManager& ) = delete;
	IndexCacheManager( IndexCacheManager&& ) = delete;
	IndexCacheManager& operator=( const IndexCacheManager& ) = delete;
	IndexCacheManager& operator=( IndexCacheManager&& ) = delete;

	/**
	 * @param type the data-type of the cache
	 * @return the cache-instance for the given type
	 */
	IndexCache& get_cache( CacheType type );

	/**
	 * Processes a node handshake by placing all of the node's cached items
	 * in the according caches.
	 * @param node_id the id of the newly connected node
	 */
	void process_handshake( uint32_t node_id, const NodeHandshake &hs);

	/**
	 * Checks whether a global reorganization is required
	 */
	bool require_reorg(const std::map<uint32_t, std::shared_ptr<Node> > &nodes) const;

	void node_failed( uint32_t node_id );

	/**
	 * Updates the statistics for the cache-entries hosted at the given node
	 * @param node_id the id of the node that delivered the statistics
	 * @param stats the statistics
	 */
	void update_stats( uint32_t node_id, const NodeStats& stats );

	/**
	 * Computes the new distribution of all cache entries along all active nodes
	 * @param nodes the currently active nods
	 * @param force whether to force reorganization, even it is not required due to rules
	 * @return the reorg-commands to trigger
	 */
	std::map<uint32_t, NodeReorgDescription> reorganize(const std::map<uint32_t, std::shared_ptr<Node> > &nodes, bool force = false );

	/**
	 * Uses the reorg-strategy to determine the best node to schedule the given request on
	 * @param request the request to schedule
	 * @param nodes the currently active nodes
	 */
	uint32_t find_node_for_job( const BaseRequest &request,const std::map<uint32_t, std::shared_ptr<Node> > &nodes ) const;

private:
	/**
	 * Retrieves the CacheInfo for the given type
	 * @param type the type to get the info for
	 */
	const CacheInfo &get_info( CacheType type ) const;

	std::vector<std::reference_wrapper<CacheInfo>> all_caches;
	CacheInfo raster_cache;
	CacheInfo point_cache;
	CacheInfo line_cache;
	CacheInfo poly_cache;
	CacheInfo plot_cache;
};



#endif /* INDEX_INDEX_CACHE_MANAGER_H_ */
