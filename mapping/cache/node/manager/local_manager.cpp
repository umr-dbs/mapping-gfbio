/*
 * local_node_manager.cpp
 *
 *  Created on: 23.05.2016
 *      Author: koerberm
 */


#include "cache/node/manager/local_manager.h"
#include "cache/node/manager/local_replacement.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"



template<class T>
LocalCacheWrapper<T>::LocalCacheWrapper(LocalCacheManager &mgr, const std::string &repl, size_t size, CacheType type ) :
	NodeCacheWrapper<T>(mgr, size, type ), mgr(mgr), replacement(make_unique<LocalReplacement<T>>( LocalRelevanceFunction::by_name(repl))) {
}

template<class T>
bool LocalCacheWrapper<T>::put(const std::string &semantic_id,
		const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler) {
	size_t size = SizeUtil::get_byte_size(*item);

	this->stats.add_result_bytes(size);

	if ( mgr.get_strategy().do_cache(profiler,size) && size <= this->cache.get_max_size() ) {
		CacheCube cube(*item);
		// Min/Max resolution hack
		if ( query.restype == QueryResolution::Type::PIXELS ) {
			double scale_x = (query.x2-query.x1) / query.xres;
			double scale_y = (query.y2-query.y1) / query.yres;

			// Result was max
			if ( scale_x < cube.resolution_info.pixel_scale_x.a )
				cube.resolution_info.pixel_scale_x.a = 0;
			// Result was minimal
			else if ( scale_x > cube.resolution_info.pixel_scale_x.b )
				cube.resolution_info.pixel_scale_x.b = std::numeric_limits<double>::infinity();


			// Result was max
			if ( scale_y < cube.resolution_info.pixel_scale_y.a )
				cube.resolution_info.pixel_scale_y.a = 0;
			// Result was minimal
			else if ( scale_y > cube.resolution_info.pixel_scale_y.b )
				cube.resolution_info.pixel_scale_y.b = std::numeric_limits<double>::infinity();
		}
		// Perform put
		Log::trace("Adding item to local cache");
		{
			std::lock_guard<std::mutex> g(rem_mtx);
			auto rems = replacement->get_removals(this->cache,size);
			for ( auto &r : rems ) {
				Log::trace("Dropping entry due to space requirement: %s", r.NodeCacheKey::to_string().c_str());
				this->cache.remove(r);
			}
		}
		this->cache.put(semantic_id,item,CacheEntry( cube, size + sizeof(NodeCacheEntry<T>), profiler));
		return true;
	}
	return false;
}

template<class T>
std::unique_ptr<T> LocalCacheWrapper<T>::query(GenericOperator& op,
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


		return PuzzleUtil::process(op,rect,qres.remainder,items,profiler);
	}
	else {
		this->stats.add_miss();
		throw NoSuchElementException("MISS");
	}
}


template<class T>
MetaCacheEntry LocalCacheWrapper<T>::put_local(
		const std::string& semantic_id, const std::unique_ptr<T>& item,
		CacheEntry&& info) {
	(void) semantic_id;
	(void) item;
	(void) info;
	throw MustNotHappenException("No external local puts allowed in local cache manager!");
}

template<class T>
void LocalCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	(void) key;
	throw MustNotHappenException("No external removals allowed in local cache manager!");
}

template<class T>
std::unique_ptr<T> LocalCacheWrapper<T>::process_puzzle(
		const PuzzleRequest& request, QueryProfiler &parent_profiler) {
	(void) request;
	(void) parent_profiler;
	throw MustNotHappenException("No external puzzling allowed in local cache manager!");
}



//
// MGR
//


LocalCacheManager::LocalCacheManager(const std::string &strategy, const std::string &replacement,
		size_t raster_cache_size, size_t point_cache_size,
		size_t line_cache_size, size_t polygon_cache_size,
		size_t plot_cache_size ) :
				NodeCacheManager( strategy,
						make_unique<LocalCacheWrapper<GenericRaster>>(*this, replacement, raster_cache_size, CacheType::RASTER),
						make_unique<LocalCacheWrapper<PointCollection>>(*this, replacement,point_cache_size, CacheType::POINT),
						make_unique<LocalCacheWrapper<LineCollection>>(*this, replacement,line_cache_size, CacheType::LINE),
						make_unique<LocalCacheWrapper<PolygonCollection>>(*this, replacement,polygon_cache_size, CacheType::POLYGON),
						make_unique<LocalCacheWrapper<GenericPlot>>(*this, replacement,plot_cache_size, CacheType::PLOT) ) {
}

template class LocalCacheWrapper<GenericRaster>;
template class LocalCacheWrapper<PointCollection>;
template class LocalCacheWrapper<LineCollection>;
template class LocalCacheWrapper<PolygonCollection>;
template class LocalCacheWrapper<GenericPlot> ;
