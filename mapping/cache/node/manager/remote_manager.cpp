/*
 * util.cpp
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#include "cache/node/manager/remote_manager.h"
#include "cache/priv/connection.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include "util/log.h"

////////////////////////////////////////////////////////////
//
// NodeCacheWrapper
//
////////////////////////////////////////////////////////////

template<typename T>
RemoteCacheWrapper<T>::RemoteCacheWrapper(RemoteCacheManager &mgr, size_t size,
		CacheType type) :
		NodeCacheWrapper<T>(mgr, size, type), mgr(mgr), retriever(*this, mgr) {
}

template<typename T>
bool RemoteCacheWrapper<T>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query,
		const QueryProfiler &profiler) {

	TIME_EXEC("CacheManager.put");

	auto &idx_con = mgr.get_worker_context().get_index_connection();

	size_t size = SizeUtil::get_byte_size(*item);
  if ( size > 25 * 1024 * 1024 )
      Log::info("Big result found: %lu", size);

	this->stats.add_result_bytes(size);

	if (mgr.get_strategy().do_cache(profiler, size)) {
		if (this->cache.get_current_size() + size
				> this->cache.get_max_size() * 1.1) {
			this->stats.add_lost_put();
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
std::unique_ptr<T> RemoteCacheWrapper<T>::query(GenericOperator& op,
		const QueryRectangle& rect, QueryProfiler &profiler) {
	if ( op.getDepth() == 0 || mgr.get_worker_context().get_puzzle_depth() > op.getDepth() )
		throw NoSuchElementException("No query");

	TIME_EXEC("CacheManager.query");
	Log::debug("Querying item: %s on %s",
			CacheCommon::qr_to_string(rect).c_str(),
			op.getSemanticId().c_str());

	// Local lookup
	CacheQueryResult < NodeCacheEntry < T >> qres = this->cache.query(op.getSemanticId(), rect);
	// Only process locally if there is no remainder
	if (!qres.has_remainder()) {
		this->stats.add_query(qres.hit_ratio);

		// Track costs
		for (auto &e : qres.items)
			profiler.addTotalCosts(e->profile);

		if (qres.items.size() == 1) {
			this->stats.add_single_local_hit();
			return qres.items.front()->copy_data();
		}
		// puzzle
		else {
			this->stats.add_multi_local_hit();
			std::vector<std::shared_ptr<const T>> items;
			items.reserve(qres.items.size());
			for (auto &ne : qres.items) {
				items.push_back(ne->data);
			}
			return PuzzleUtil::process(op, rect, qres.remainder, items, profiler);
		}
	}

	// Remote lookup

	// Local miss... asking index
	TIME_EXEC2("CacheManager.query.remote");
	Log::debug("Local MISS for query: %s on %s. Querying index.",
			CacheCommon::qr_to_string(rect).c_str(),
			op.getSemanticId().c_str());
	BaseRequest cr(CacheType::RASTER, op.getSemanticId(), rect);

	std::unique_ptr<BinaryReadBuffer> resp =
			mgr.get_worker_context().get_index_connection().write_and_read(
					WorkerConnection::CMD_QUERY_CACHE, cr);
	uint8_t rc = resp->read<uint8_t>();

	switch (rc) {
	// Full hit on different client
	case WorkerConnection::RESP_QUERY_HIT: {
		this->stats.add_single_remote_hit();
		Log::trace(
				"Full single remote HIT for query: %s on %s. Returning cached raster.",
				CacheCommon::qr_to_string(rect).c_str(),
				op.getSemanticId().c_str());
		try {
			return retriever.load(op.getSemanticId(), CacheRef(*resp), profiler);
		} catch ( const DeliveryException &de ) {
			throw NoSuchElementException("Remote-entry gone!");
		}
	}
		// Full miss on whole cache
	case WorkerConnection::RESP_QUERY_MISS: {
		this->stats.add_miss();
		Log::trace("Full remote MISS for query: %s on %s.",
				CacheCommon::qr_to_string(rect).c_str(),
				op.getSemanticId().c_str());
		throw NoSuchElementException("Cache-Miss.");
	}
		// Puzzle time
	case WorkerConnection::RESP_QUERY_PARTIAL: {
		PuzzleRequest pr(*resp);

		// STATS ONLY
		bool local_only = true;
		for (auto &ref : pr.parts) {
			local_only &= mgr.is_local_ref(ref);
		}
		if (local_only)
			this->stats.add_multi_local_partial();
		else if (pr.has_remainders())
			this->stats.add_multi_remote_partial();
		else
			this->stats.add_multi_remote_hit();
		// END STATS ONLY

		Log::trace("Partial remote HIT for query: %s on %s: %s",
				CacheCommon::qr_to_string(rect).c_str(),
				op.getSemanticId().c_str(), pr.to_string().c_str());
		return process_puzzle_int(op,pr, profiler);
		break;
	}
	default: {
		throw NetworkException("Received unknown response from index.");
	}
	}
}

template<typename T>
MetaCacheEntry RemoteCacheWrapper<T>::put_local(const std::string& semantic_id,
		const std::unique_ptr<T>& item, CacheEntry &&info) {
	TIME_EXEC("CacheManager.put.local");
	Log::debug("Adding item to local cache");
	return this->cache.put(semantic_id, item, info);
}

template<typename T>
void RemoteCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	Log::debug("Removing item from local cache. Key: %s",
			key.to_string().c_str());
	this->cache.remove(key);
}

template<typename T>
std::unique_ptr<T> RemoteCacheWrapper<T>::process_puzzle(
		const PuzzleRequest& request, QueryProfiler &parent_profiler) {

	std::unique_ptr<T> result;
	QueryProfiler profiler;
	{
		auto op = GenericOperator::fromJSON(request.semantic_id);
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = process_puzzle_int(*op,request, profiler);
	}
	return result;
}

template<typename T>
std::unique_ptr<T> RemoteCacheWrapper<T>::process_puzzle_int( GenericOperator &op,
		const PuzzleRequest& request, QueryProfiler& profiler) {

	TIME_EXEC("CacheManager.puzzle");

	std::vector<std::shared_ptr<const T>> parts;

	std::vector<Cube<3>> rems;
	rems.insert(rems.begin(), request.remainder.begin(), request.remainder.end());

	for (auto &ref : request.parts) {
		try {
			parts.push_back(retriever.fetch(request.semantic_id, ref, profiler));
		} catch ( const NoSuchElementException &nse ) {
			Log::debug("Puzzle-piece gone, adding to remainders");
			SpatialReference sref(request.query.epsg,ref.bounds.get_dimension(0).a,ref.bounds.get_dimension(1).a,
					ref.bounds.get_dimension(0).b,ref.bounds.get_dimension(1).b);

			rems.push_back(ref.bounds);
		}
	}

	// All parts gone!
	if ( parts.empty() ) {
		throw NoSuchElementException("All puzzle pieces gone!");
	}
	else {
		PuzzleGuard pg(mgr.get_worker_context());
		return PuzzleUtil::process(op, request.query, request.remainder, parts, profiler);
	}
}

////////////////////////////////////////////////////////////
//
// NodeCacheManager
//
////////////////////////////////////////////////////////////

RemoteCacheManager::RemoteCacheManager(const std::string &strategy,
		size_t raster_cache_size, size_t point_cache_size,
		size_t line_cache_size, size_t polygon_cache_size,
		size_t plot_cache_size) :
		NodeCacheManager(strategy,
				make_unique<RemoteCacheWrapper<GenericRaster>>(*this,
						raster_cache_size, CacheType::RASTER),
				make_unique<RemoteCacheWrapper<PointCollection>>(*this,
						point_cache_size, CacheType::POINT),
				make_unique<RemoteCacheWrapper<LineCollection>>(*this,
						line_cache_size, CacheType::LINE),
				make_unique<RemoteCacheWrapper<PolygonCollection>>(*this,
						polygon_cache_size, CacheType::POLYGON),
				make_unique<RemoteCacheWrapper<GenericPlot>>(*this,
						plot_cache_size, CacheType::PLOT)) {
}

CacheRef RemoteCacheManager::create_local_ref(uint64_t id, const Cube<3> &bounds) const {
	return CacheRef(my_host, my_port, id,bounds);
}

bool RemoteCacheManager::is_local_ref(const CacheRef& ref) const {
	return ref.host == my_host && ref.port == my_port;
}

template class RemoteCacheWrapper<GenericRaster> ;
template class RemoteCacheWrapper<PointCollection> ;
template class RemoteCacheWrapper<LineCollection> ;
template class RemoteCacheWrapper<PolygonCollection> ;
template class RemoteCacheWrapper<GenericPlot> ;
