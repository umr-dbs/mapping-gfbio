/*
 * util.cpp
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#include "cache/node/node_manager.h"
#include "cache/manager.h"
#include "cache/priv/connection.h"
#include "util/nio.h"


WorkerContext::WorkerContext() : puzzling(false), index_connection(nullptr) {
}

bool WorkerContext::is_puzzling() const {
	return puzzling;
}

BinaryStream& WorkerContext::get_index_connection() const {
	if ( index_connection == nullptr )
		throw IllegalStateException("No index-connection configured for this thread");
	return *index_connection;
}

void WorkerContext::set_index_connection(BinaryStream* stream) {
	index_connection = stream;
}

//
// Node Wrapper
//

template<typename T>
NodeCacheWrapper<T>::NodeCacheWrapper( NodeCacheManager &mgr, NodeCache<T> &cache,
		std::unique_ptr<RemoteRetriever<T>> retriever,
		std::unique_ptr<Puzzler<T>> puzzler,
		const CachingStrategy &strategy ) :
	mgr(mgr), cache(cache),
	retriever(std::move(retriever)),
	puzzle_util( make_unique<PuzzleUtil<T>>(*this->retriever, std::move(puzzler)) ),
	strategy(strategy) {
}

template<typename T>
bool NodeCacheWrapper<T>::put(const std::string& semantic_id, const std::unique_ptr<T>& item, const QueryRectangle &query, const QueryProfiler &profiler) {
	// Do nothing if we are puzzling --> Do not cache remainder queries
	if ( mgr.get_worker_context().is_puzzling() )
		return false;


	TIME_EXEC("CacheManager.put");

	BinaryStream &stream = mgr.get_worker_context().get_index_connection();

	size_t size = SizeUtil::get_byte_size(*item);

	if ( strategy.do_cache(profiler,size) ) {
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

		auto ref = put_local(semantic_id, item, CacheEntry( cube, size + sizeof(NodeCacheEntry<T>), profiler) );
		TIME_EXEC("CacheManager.put.remote");

		Log::debug("Adding item to remote cache: %s", ref.to_string().c_str());
		buffered_write(stream,WorkerConnection::RESP_NEW_CACHE_ENTRY,ref);
		return true;
	}
	else {
		Log::debug("Item will not be cached according to strategy");
		return false;
	}
}

template<typename T>
NodeCacheRef NodeCacheWrapper<T>::put_local(const std::string& semantic_id,
	const std::unique_ptr<T>& item, CacheEntry &&info) {
	TIME_EXEC("CacheManager.put.local");
	Log::debug("Adding item to local cache");
	return cache.put(semantic_id, item, info);
}

template<typename T>
void NodeCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	Log::debug("Removing item from local cache. Key: %s", key.to_string().c_str());
	ExclusiveLockGuard g(local_lock);
	cache.remove(key);
}

template<typename T>
std::shared_ptr<const NodeCacheEntry<T>> NodeCacheWrapper<T>::get(const NodeCacheKey &key) {
	Log::debug("Getting item from local cache. Key: %s", key.to_string().c_str());
	return cache.get(key);
}

//template<typename T>
//const std::shared_ptr<const T> NodeCacheWrapper<T>::get_ref(const NodeCacheKey& key) {
//	Log::debug("Getting item from local cache. Key: %s", key.to_string().c_str());
//	return cache.get(key);
//}
//
//template<typename T>
//NodeCacheRef NodeCacheWrapper<T>::get_entry_info(const NodeCacheKey& key) {
//	return cache.get_entry_metadata(key);
//}

template<typename T>
std::unique_ptr<T> NodeCacheWrapper<T>::query(const GenericOperator& op, const QueryRectangle& rect, QueryProfiler &profiler) {
	if ( op.getDepth() == 0 ) {
		Log::debug("Graph-Depth = 0, omitting index query, returning MISS.");
		throw NoSuchElementException("Cache-Miss");
	}

	TIME_EXEC("CacheManager.query");
	Log::debug("Querying item: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	{
		SharedLockGuard g(local_lock);
		// Local lookup
		CacheQueryResult<uint64_t> qres = cache.query(op.getSemanticId(), rect);

		Log::debug("QueryResult: %s", qres.to_string().c_str() );

		// Full single local hit
		if ( !qres.has_remainder() && qres.keys.size() == 1 ) {
			stats.add_single_local_hit();
			TIME_EXEC("CacheManager.query.full_single_hit");
			Log::trace("Full single local HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			NodeCacheKey key(op.getSemanticId(), qres.keys.at(0));
			auto e = cache.get(key);
			profiler.addTotalCosts(e->profile);
			return e->copy_data();
		}
		// Full local hit (puzzle)
		else if ( !qres.has_remainder() ) {
			stats.add_multi_local_hit();
			TIME_EXEC("CacheManager.query.full_local_hit");
			Log::trace("Full local HIT for query: %s on %s. Puzzling result.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			std::vector<CacheRef> refs;
			for ( auto &id : qres.keys )
				refs.push_back( mgr.create_self_ref(id) );

			PuzzleRequest pr( cache.type, op.getSemanticId(), rect, qres.remainder, refs );
			return process_puzzle(pr,profiler);
		}
	}

	// Local miss... asking index
	TIME_EXEC2("CacheManager.query.remote");
	Log::debug("Local MISS for query: %s on %s. Querying index.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
	BaseRequest cr(CacheType::RASTER, op.getSemanticId(), rect);
	BinaryStream &stream = mgr.get_worker_context().get_index_connection();

	buffered_write(stream,WorkerConnection::CMD_QUERY_CACHE,cr);
	uint8_t resp;
	stream.read(&resp);
	switch (resp) {
		// Full hit on different client
		case WorkerConnection::RESP_QUERY_HIT: {
			stats.add_single_remote_hit();
			Log::trace("Full single remote HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			return retriever->load( op.getSemanticId(), CacheRef(stream), profiler );
		}
		// Full miss on whole cache
		case WorkerConnection::RESP_QUERY_MISS: {
			stats.add_miss();
			Log::trace("Full remote MISS for query: %s on %s.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			throw NoSuchElementException("Cache-Miss.");
			break;
		}
		// Puzzle time
		case WorkerConnection::RESP_QUERY_PARTIAL: {
			PuzzleRequest pr(stream);

			// STATS ONLY
			bool local_only = true;
			for ( auto &ref : pr.parts ) {
				local_only &= mgr.is_self_ref(ref);
			}
			if ( local_only )
				stats.add_multi_local_partial();
			else if ( pr.has_remainders() )
				stats.add_multi_remote_partial();
			else
				stats.add_multi_remote_hit();
			// END STATS ONLY

			Log::trace("Partial remote HIT for query: %s on %s: %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str(), pr.to_string().c_str() );
			return process_puzzle(pr,profiler);
			break;
		}
		default: {
			throw NetworkException("Received unknown response from index.");
		}
	}
}

template<typename T>
std::unique_ptr<T> NodeCacheWrapper<T>::process_puzzle(const PuzzleRequest& request, QueryProfiler &parent_profiler) {
	TIME_EXEC("CacheManager.puzzle");
	std::unique_ptr<T> result;
	QueryProfiler profiler;
	{
		PuzzleGuard pg(mgr.get_worker_context());
		QueryProfilerRunningGuard guard(parent_profiler,profiler);
		result = puzzle_util->process_puzzle(request,profiler);
	}
	if ( put( request.semantic_id, result, request.query, profiler ) )
		parent_profiler.cached(profiler);
	return result;
}


////////////////////////////////////////////////////////////
//
// MANAGER STUFF
//
////////////////////////////////////////////////////////////

thread_local WorkerContext NodeCacheManager::context;

NodeCacheManager::NodeCacheManager( std::unique_ptr<CachingStrategy> strategy,
	size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
	size_t polygon_cache_size, size_t plot_cache_size) :
	raster_cache(CacheType::RASTER,raster_cache_size),
	point_cache(CacheType::POINT,point_cache_size),
	line_cache(CacheType::LINE,line_cache_size),
	polygon_cache(CacheType::POLYGON,polygon_cache_size),
	plot_cache(CacheType::PLOT,plot_cache_size),
	raster_wrapper(*this,raster_cache, make_unique<RemoteRetriever<GenericRaster>>(raster_cache,*this), make_unique<RasterPuzzler>(), *strategy),
	point_wrapper(*this,point_cache, make_unique<RemoteRetriever<PointCollection>>(point_cache,*this), make_unique<PointCollectionPuzzler>(), *strategy),
	line_wrapper(*this,line_cache, make_unique<RemoteRetriever<LineCollection>>(line_cache,*this), make_unique<LineCollectionPuzzler>(), *strategy),
	polygon_wrapper(*this,polygon_cache, make_unique<RemoteRetriever<PolygonCollection>>(polygon_cache,*this), make_unique<PolygonCollectionPuzzler>(), *strategy),
	plot_wrapper(*this,plot_cache, make_unique<RemoteRetriever<GenericPlot>>(plot_cache,*this), make_unique<PlotPuzzler>(), *strategy),
	strategy(std::move(strategy)),
	my_port(0) {
}

NodeCacheWrapper<GenericRaster>& NodeCacheManager::get_raster_cache() {
	return raster_wrapper;
}

NodeCacheWrapper<PointCollection>& NodeCacheManager::get_point_cache() {
	return point_wrapper;
}

NodeCacheWrapper<LineCollection>& NodeCacheManager::get_line_cache() {
	return line_wrapper;
}

NodeCacheWrapper<PolygonCollection>& NodeCacheManager::get_polygon_cache() {
	return polygon_wrapper;
}

NodeCacheWrapper<GenericPlot>& NodeCacheManager::get_plot_cache() {
	return plot_wrapper;
}

const QueryStats& NodeCacheManager::get_query_stats() const {
	return cumulated_stats;
}

void NodeCacheManager::reset_query_stats() {
	raster_wrapper.stats.get_and_reset();
	point_wrapper.stats.get_and_reset();
	line_wrapper.stats.get_and_reset();
	polygon_wrapper.stats.get_and_reset();
	plot_wrapper.stats.get_and_reset();
	cumulated_stats.reset();
}

void NodeCacheManager::set_self_port(uint32_t port) {
	my_port = port;
}

void NodeCacheManager::set_self_host(const std::string& host) {
	my_host = host;
}

CacheRef NodeCacheManager::create_self_ref(uint64_t id) const {
	return CacheRef( my_host, my_port, id );
}

bool NodeCacheManager::is_self_ref(const CacheRef& ref) const {
	return ref.host == my_host && ref.port == my_port;
}

NodeHandshake NodeCacheManager::create_handshake() const {
	Capacity cap(
		raster_cache.get_max_size(), raster_cache.get_current_size(),
		point_cache.get_max_size(), point_cache.get_current_size(),
		line_cache.get_max_size(), line_cache.get_current_size(),
		polygon_cache.get_max_size(), polygon_cache.get_current_size(),
		plot_cache.get_max_size(), plot_cache.get_current_size()
	);

	std::vector<NodeCacheRef> entries = raster_cache.get_all();
	std::vector<NodeCacheRef> tmp = point_cache.get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = line_cache.get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = polygon_cache.get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = plot_cache.get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );
	return NodeHandshake(my_port, cap, entries );
}

NodeStats NodeCacheManager::get_stats() const {
	Capacity cap(
		raster_cache.get_max_size(), raster_cache.get_current_size(),
		point_cache.get_max_size(), point_cache.get_current_size(),
		line_cache.get_max_size(), line_cache.get_current_size(),
		polygon_cache.get_max_size(), polygon_cache.get_current_size(),
		plot_cache.get_max_size(), plot_cache.get_current_size()
	);

	QueryStats qs;
	qs += raster_wrapper.stats.get_and_reset();
	qs += point_wrapper.stats.get_and_reset();
	qs += line_wrapper.stats.get_and_reset();
	qs += polygon_wrapper.stats.get_and_reset();
	qs += plot_wrapper.stats.get_and_reset();

	cumulated_stats += qs;
	std::vector<CacheStats> stats {
		raster_cache.get_stats(),
		point_cache.get_stats(),
		line_cache.get_stats(),
		polygon_cache.get_stats(),
		plot_cache.get_stats()
	};
	return NodeStats( cap, qs, stats );
}

WorkerContext& NodeCacheManager::get_worker_context() {
	return NodeCacheManager::context;
}


template class NodeCacheWrapper<GenericRaster>;
template class NodeCacheWrapper<PointCollection>;
template class NodeCacheWrapper<LineCollection>;
template class NodeCacheWrapper<PolygonCollection>;
template class NodeCacheWrapper<GenericPlot> ;

