/*
 * util.cpp
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#include "cache/node/manager/hybrid_manager.h"
#include "cache/priv/connection.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

////////////////////////////////////////////////////////////
//
// NodeCacheWrapper
//
////////////////////////////////////////////////////////////

template<typename T>
HybridCacheWrapper<T>::HybridCacheWrapper(HybridCacheManager&mgr, size_t size,
		CacheType type) :
		NodeCacheWrapper<T>(mgr, size, type), mgr(mgr) {
}

template<typename T>
bool HybridCacheWrapper<T>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query,
		const QueryProfiler &profiler) {

	TIME_EXEC("CacheManager.put");

	auto &idx_con = mgr.get_worker_context().get_index_connection();

	size_t size = SizeUtil::get_byte_size(*item);
	this->stats.add_result_bytes(size);

	if (mgr.get_strategy().do_cache(profiler, size)) {
		if (this->cache.get_current_size() + size
				> this->cache.get_max_size() * 1.1) {
			Log::debug("Not caching item, buffer due to overflow");
			return false;
		}

		CacheCube cube(*item);
		// Min/Max resolution hack
		if (query.restype == QueryResolution::Type::PIXELS) {
			double scale_x = (query.x2 - query.x1) / query.xres;
			double scale_y = (query.y2 - query.y1) / query.yres;

			// Result was max
			if (scale_x < cube.resolution_info.pixel_scale_x.a)
				cube.resolution_info.pixel_scale_x.a = 0;
			// Result was minimal
			else if (scale_x > cube.resolution_info.pixel_scale_x.b)
				cube.resolution_info.pixel_scale_x.b = std::numeric_limits<
						double>::infinity();

			// Result was max
			if (scale_y < cube.resolution_info.pixel_scale_y.a)
				cube.resolution_info.pixel_scale_y.a = 0;
			// Result was minimal
			else if (scale_y > cube.resolution_info.pixel_scale_y.b)
				cube.resolution_info.pixel_scale_y.b = std::numeric_limits<
						double>::infinity();
		}

		auto ref = put_local(semantic_id, item,
				CacheEntry(cube, size + sizeof(NodeCacheEntry<T> ), profiler));
		TIME_EXEC("CacheManager.put.remote");

		Log::debug("Adding item to remote cache: %s", ref.to_string().c_str());
		idx_con.write(WorkerConnection::RESP_NEW_CACHE_ENTRY, ref);
		return true;
	} else {
		Log::debug("Item will not be cached according to strategy");
		return false;
	}
}

template<typename T>
std::unique_ptr<T> HybridCacheWrapper<T>::query(GenericOperator& op,
		const QueryRectangle& rect, QueryProfiler &profiler) {

	CacheQueryResult<NodeCacheEntry<T>> qres = this->cache.query(op.getSemanticId(), rect);
	for ( auto &e : qres.items ) {
		// Track costs
		profiler.addTotalCosts(e->profile);
	}

	this->stats.add_query(qres.hit_ratio);

	// Full single local hit
	if ( !qres.has_remainder() && qres.items.size() == 1 ) {
		this->stats.add_single_local_hit();
		return qres.items.front()->copy_data();
	}
	// Partial or Full puzzle
	else if ( qres.has_hit() ) {
		if ( qres.has_remainder() )
			this->stats.add_multi_local_partial();
		else
			this->stats.add_multi_local_hit();

		std::vector<std::shared_ptr<const T>> items;
		items.reserve(qres.items.size());
		for ( auto &ne : qres.items )
			items.push_back(ne->data);

		return PuzzleJob::process(op,rect,qres.remainder,items,profiler);
	}
	else {
		this->stats.add_miss();
		throw NoSuchElementException("MISS");
	}
}

template<typename T>
MetaCacheEntry HybridCacheWrapper<T>::put_local(const std::string& semantic_id,
		const std::unique_ptr<T>& item, CacheEntry &&info) {
	TIME_EXEC("CacheManager.put.local");
	Log::debug("Adding item to local cache");
	return this->cache.put(semantic_id, item, info);
}

template<typename T>
void HybridCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	Log::debug("Removing item from local cache. Key: %s",
			key.to_string().c_str());
	this->cache.remove(key);
}

template<typename T>
std::unique_ptr<T> HybridCacheWrapper<T>::process_puzzle(
		const PuzzleRequest& request, QueryProfiler &parent_profiler) {
	(void) request;
	(void) parent_profiler;
	throw MustNotHappenException("No external puzzling allowed in local cache manager!");
}


////////////////////////////////////////////////////////////
//
// NodeCacheManager
//
////////////////////////////////////////////////////////////

HybridCacheManager::HybridCacheManager(const std::string &strategy,
		size_t raster_cache_size, size_t point_cache_size,
		size_t line_cache_size, size_t polygon_cache_size,
		size_t plot_cache_size) :
		NodeCacheManager(strategy,
				make_unique<HybridCacheWrapper<GenericRaster>>(*this,
						raster_cache_size, CacheType::RASTER),
				make_unique<HybridCacheWrapper<PointCollection>>(*this,
						point_cache_size, CacheType::POINT),
				make_unique<HybridCacheWrapper<LineCollection>>(*this,
						line_cache_size, CacheType::LINE),
				make_unique<HybridCacheWrapper<PolygonCollection>>(*this,
						polygon_cache_size, CacheType::POLYGON),
				make_unique<HybridCacheWrapper<GenericPlot>>(*this,
						plot_cache_size, CacheType::PLOT)) {
}

template class HybridCacheWrapper<GenericRaster> ;
template class HybridCacheWrapper<PointCollection> ;
template class HybridCacheWrapper<LineCollection> ;
template class HybridCacheWrapper<PolygonCollection> ;
template class HybridCacheWrapper<GenericPlot> ;
