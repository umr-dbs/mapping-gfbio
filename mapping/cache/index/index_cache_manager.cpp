/*
 * index_cache_manager.cpp
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#include "cache/index/index_cache_manager.h"
#include "util/log.h"


////////////////////////////////////////////////////////////
//
// INDEX CACHES
//
////////////////////////////////////////////////////////////

IndexCacheManager::CacheInfo::CacheInfo(CacheType type, const std::string& reorg_strategy, const std::string& relevance_function) :
	cache(make_unique<IndexCache>(type) ),
	reorg_strategy( ReorgStrategy::by_name(*this->cache, reorg_strategy, relevance_function)){
}

IndexCacheManager::IndexCacheManager(const IndexConfig &config) :
	raster_cache(CacheType::RASTER,config.reorg_strategy,config.relevance_function),
	point_cache(CacheType::POINT,config.reorg_strategy,config.relevance_function),
	line_cache(CacheType::LINE,config.reorg_strategy,config.relevance_function),
	poly_cache(CacheType::POLYGON,config.reorg_strategy,config.relevance_function),
	plot_cache(CacheType::PLOT,config.reorg_strategy,config.relevance_function){

	all_caches.push_back(raster_cache);
	all_caches.push_back(point_cache);
	all_caches.push_back(line_cache);
	all_caches.push_back(poly_cache);
	all_caches.push_back(plot_cache);
}

void IndexCacheManager::node_failed(uint32_t node_id) {
	for ( CacheInfo &c : all_caches ) {
		c.cache->remove_all_by_node(node_id);
		c.reorg_strategy->node_failed(node_id);
	}
}

const IndexCacheManager::CacheInfo& IndexCacheManager::get_info(CacheType type) const {
	switch ( type ) {
		case CacheType::RASTER:
			return raster_cache;
		case CacheType::POINT:
			return point_cache;
		case CacheType::LINE:
			return line_cache;
		case CacheType::POLYGON:
			return poly_cache;
		case CacheType::PLOT:
			return plot_cache;
		default:
			throw ArgumentException(concat("Unknown cache-type: ",(int)type));
	}
}

IndexCache& IndexCacheManager::get_cache(CacheType type) {
	return *get_info(type).cache;
}

bool IndexCacheManager::require_reorg(const std::map<uint32_t, std::shared_ptr<Node> > &nodes) const {
	for ( CacheInfo &c : all_caches )
		if ( c.reorg_strategy->requires_reorg(nodes) )
			return true;
	return false;
}

void IndexCacheManager::process_handshake(uint32_t node_id, const NodeHandshake& hs) {
	for (auto &content : hs.get_data()) {
		auto &cache = get_info(content.type);
		for ( auto &p : content.get_items() ) {
			for ( auto &entry : p.second )
				cache.cache->put( p.first, node_id, entry.entry_id, entry );
		}
	}
}

void IndexCacheManager::update_stats(uint32_t node_id, const NodeStats& stats) {
	for ( auto &s : stats.stats ) {
		get_cache(s.type).update_stats(node_id,s);
	}
}

uint32_t IndexCacheManager::find_node_for_job(const BaseRequest& request,const std::map<uint32_t, std::shared_ptr<Node> > &nodes) const {
	return get_info(request.type).reorg_strategy->get_node_for_job(request,nodes);
}

std::map<uint32_t, NodeReorgDescription> IndexCacheManager::reorganize(const std::map<uint32_t, std::shared_ptr<Node> > &nodes, bool force) {
	Log::debug("Calculating reorganization of cache");

	std::map<uint32_t, NodeReorgDescription> result;
	for (auto &kv : nodes)
		result.emplace(kv.first, NodeReorgDescription(kv.second));

	for ( CacheInfo &c : all_caches ) {
		if ( c.reorg_strategy->requires_reorg(nodes) || force )
			c.reorg_strategy->reorganize(result);
	}
	Log::debug("Finished calculating reorganization of cache");
	return result;
}
