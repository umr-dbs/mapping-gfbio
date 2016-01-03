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


class AttributeArraysHelper {
public:
	static void append( AttributeArrays &dest, const AttributeArrays &src );
private:
	template <typename T>
	static void append_arr( AttributeArrays::AttributeArray<T> &dest, const AttributeArrays::AttributeArray<T> &src );
};

void AttributeArraysHelper::append( AttributeArrays &dest, const AttributeArrays &src ) {
	for (auto &n : dest._numeric) {
		append_arr( n.second, src._numeric.at(n.first) );
	}
	for (auto &n : dest._textual) {
		append_arr( n.second, src._textual.at(n.first) );
	}
}

template<typename T>
void AttributeArraysHelper::append_arr(
		AttributeArrays::AttributeArray<T>& dest,
		const AttributeArrays::AttributeArray<T>& src) {
	dest.reserve(dest.array.size() + src.array.size() );
	dest.array.insert(dest.array.end(), src.array.begin(), src.array.end() );
}

//
// Node Wrapper
//

template<typename T>
NodeCacheWrapper<T>::NodeCacheWrapper( const NodeCacheManager &mgr, NodeCache<T>& cache, const CachingStrategy &strategy) :
	mgr(mgr), cache(cache), strategy(strategy) {
}

template<typename T>
void NodeCacheWrapper<T>::put(const std::string& semantic_id, const std::unique_ptr<T>& item, const QueryRectangle &query, const QueryProfiler &profiler) {

	ExecTimer t("CacheManager.put");

	BinaryStream &stream = mgr.get_index_connection();

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

		auto ref = put_local(semantic_id, item, CacheEntry( cube, size, strategy.get_costs(profiler,size)) );
		ExecTimer t("CacheManager.put.remote");

		Log::debug("Adding item to remote cache: %s", ref.to_string().c_str());
		buffered_write(stream,WorkerConnection::RESP_NEW_CACHE_ENTRY,ref);
	}
	else
		Log::debug("Item will not be cached according to strategy");
}

template<typename T>
NodeCacheRef NodeCacheWrapper<T>::put_local(const std::string& semantic_id,
	const std::unique_ptr<T>& item, CacheEntry &&info) {
	ExecTimer t("CacheManager.put.local");
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
const std::shared_ptr<const T> NodeCacheWrapper<T>::get_ref(const NodeCacheKey& key) {
	Log::debug("Getting item from local cache. Key: %s", key.to_string().c_str());
	return cache.get(key);
}

template<typename T>
NodeCacheRef NodeCacheWrapper<T>::get_entry_info(const NodeCacheKey& key) {
	return cache.get_entry_metadata(key);
}

template<typename T>
std::unique_ptr<T> NodeCacheWrapper<T>::query(const GenericOperator& op, const QueryRectangle& rect) {
	if ( op.getDepth() == 0 ) {
		Log::debug("Graph-Depth = 0, omitting index query, returning MISS.");
		throw NoSuchElementException("Cache-Miss");
	}

	ExecTimer t("CacheManager.query");
	Log::debug("Querying item: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	{
		SharedLockGuard g(local_lock);
		// Local lookup
		CacheQueryResult<uint64_t> qres = cache.query(op.getSemanticId(), rect);

		Log::debug("QueryResult: %s", qres.to_string().c_str() );

		// Full single local hit
		if ( !qres.has_remainder() && qres.keys.size() == 1 ) {
			ExecTimer t("CacheManager.query.full_single_hit");
			Log::trace("Full single local HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			NodeCacheKey key(op.getSemanticId(), qres.keys.at(0));
			return cache.get_copy(key);
		}
		// Full local hit (puzzle)
		else if ( !qres.has_remainder() ) {
			ExecTimer t("CacheManager.query.full_local_hit");
			Log::trace("Full local HIT for query: %s on %s. Puzzling result.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			std::vector<CacheRef> refs;
			for ( auto &id : qres.keys )
				refs.push_back( mgr.create_self_ref(id) );

			PuzzleRequest pr( cache.type, op.getSemanticId(), rect, qres.remainder, refs );
			QueryProfiler qp;
			std::unique_ptr<T> result = process_puzzle(pr,qp);
			return result;
		}
	}

	// Local miss... asking index
	ExecTimer t2("CacheManager.query.remote");
	Log::debug("Local MISS for query: %s on %s. Querying index.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
	BaseRequest cr(CacheType::RASTER, op.getSemanticId(), rect);
	BinaryStream &stream = mgr.get_index_connection();

	buffered_write(stream,WorkerConnection::CMD_QUERY_CACHE,cr);
	uint8_t resp;
	stream.read(&resp);
	switch (resp) {
		// Full hit on different client
		case WorkerConnection::RESP_QUERY_HIT: {
			Log::trace("Full single remote HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			QueryProfiler qp;
			return fetch_item( op.getSemanticId(), CacheRef(stream), qp );
		}
		// Full miss on whole cache
		case WorkerConnection::RESP_QUERY_MISS: {
			Log::trace("Full remote MISS for query: %s on %s.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
			throw NoSuchElementException("Cache-Miss.");
			break;
		}
		// Puzzle time
		case WorkerConnection::RESP_QUERY_PARTIAL: {
			PuzzleRequest pr(stream);
			Log::trace("Partial remote HIT for query: %s on %s: %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str(), pr.to_string().c_str() );
			QueryProfiler qp;
			auto res = process_puzzle(pr,qp);
			return res;
			break;
		}
		default: {
			throw NetworkException("Received unknown response from index.");
		}
	}
}

template<typename T>
std::unique_ptr<T> NodeCacheWrapper<T>::process_puzzle(const PuzzleRequest& request, QueryProfiler &profiler) {
	ExecTimer t("CacheManager.puzzle");
	Log::trace("Processing puzzle-request: %s", request.to_string().c_str());

	std::vector<std::shared_ptr<const T>> items;

	{
		ExecTimer t("CacheManager.puzzle.fetch_parts");
		// Fetch puzzle parts
		Log::trace("Fetching all puzzle-parts");
		for (const CacheRef &cr : request.parts) {
			if ( mgr.is_self_ref(cr) ) {
				Log::trace("Fetching puzzle-piece from local cache, key: %d", cr.entry_id);
				items.push_back(get_ref( NodeCacheKey(request.semantic_id, cr.entry_id) ));
			}
			else {
				Log::debug("Fetching puzzle-piece from %s:%d, key: %d", cr.host.c_str(), cr.port, cr.entry_id);

				auto raster = fetch_item( request.semantic_id, cr, profiler );
				items.push_back( std::shared_ptr<const T>(raster.release()) );
			}
		}
	}

	// Create remainder
	Log::trace("Creating remainder queries.");
	auto ref = items.front();
	auto remainders = compute_remainders(request.semantic_id,*ref,request, profiler);

	for ( auto &r : remainders )
		items.push_back( std::shared_ptr<T>(r.release()));

	auto result = do_puzzle( enlarge_puzzle(request.query,items), items);
	put( request.semantic_id, result, request.query, profiler );
	Log::trace("Finished processing puzzle-request: %s", request.to_string().c_str());
	return result;
}

template<typename T>
SpatioTemporalReference NodeCacheWrapper<T>::enlarge_puzzle(const QueryRectangle& query,
	const std::vector<std::shared_ptr<const T> >& items) {
	ExecTimer t("CacheManager.puzzle.enlarge");

	// Calculated maximum covered rectangle
	double values[6] = { DoubleNegInfinity, DoubleInfinity,
						 DoubleNegInfinity, DoubleInfinity,
						 DoubleNegInfinity, DoubleInfinity };
	QueryCube qc(query);
	for ( auto &item : items ) {
		Cube3 ic( item->stref.x1, item->stref.x2,
				  item->stref.y1, item->stref.y2,
				  item->stref.t1, item->stref.t2 );

		for ( int i = 0; i < 3; i++ ) {
			auto &cdim = ic.get_dimension(i);
			auto &qdim = qc.get_dimension(i);
			int idx_l = 2*i;
			int idx_r = idx_l + 1;

			// If this item touches the bounds of the query.... extend
			if ( cdim.a <= qdim.a )
				values[idx_l] = std::max(values[idx_l],cdim.a);

			if ( cdim.b >= qdim.b )
				values[idx_r] = std::min(values[idx_r],cdim.b);
		}
	}

	// Final check... stupid floating point stuff
	for ( int i = 0; i < 6; i++ ) {
		if ( !std::isfinite(values[i]) ) {
			auto &d = qc.get_dimension(i/2);
			values[i] = (i%2) == 0 ? d.a : d.b;
		}
	}

	return SpatioTemporalReference(
		SpatialReference( query.epsg, values[0], values[2], values[1], values[3] ),
		TemporalReference( query.timetype, values[4], values[5] )
	);
}

// TODO: Hack for plots
template<>
SpatioTemporalReference NodeCacheWrapper<GenericPlot>::enlarge_puzzle(const QueryRectangle& query,
	const std::vector<std::shared_ptr<const GenericPlot> >& items) {
	(void) items;
	return SpatioTemporalReference(query,query);
}


template<typename T>
std::unique_ptr<T> NodeCacheWrapper<T>::fetch_item(const std::string& semantic_id, const CacheRef& ref, QueryProfiler &qp) {
	TypedNodeCacheKey key(cache.type,semantic_id,ref.entry_id);
	Log::debug("Fetching cache-entry from: %s:%d, key: %d", ref.host.c_str(), ref.port, ref.entry_id );
	UnixSocket sock(ref.host.c_str(), ref.port,true);
	BinaryStream &stream = sock;

	buffered_write(sock,DeliveryConnection::MAGIC_NUMBER,DeliveryConnection::CMD_GET_CACHED_ITEM,key);
	uint8_t resp;
	stream.read(&resp);

	switch (resp) {
		case DeliveryConnection::RESP_OK: {
			MoveInfo mi(stream);
			qp.addIOCost( mi.size );
			return read_item(stream);
		}
		case DeliveryConnection::RESP_ERROR: {
			std::string err_msg;
			stream.read(&err_msg);
			Log::error("Delivery returned error: %s", err_msg.c_str());
			throw DeliveryException(err_msg);
		}
		default: {
			Log::error("Delivery returned unknown code: %d", resp);
			throw DeliveryException("Delivery returned unknown code");
		}
	}
}

template<typename T>
std::vector<std::unique_ptr<T>> NodeCacheWrapper<T>::compute_remainders(const std::string& semantic_id,
	const T& ref_result, const PuzzleRequest &request, QueryProfiler &profiler ) {
	ExecTimer t("CacheManager.puzzle.remainer_calc");
	(void) ref_result;
	std::vector<std::unique_ptr<T>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	for ( auto &rqr : request.get_remainder_queries() ) {
		result.push_back( compute_item(*graph,rqr,profiler) );
	}
	return result;
}

template<>
std::vector<std::unique_ptr<GenericRaster>> NodeCacheWrapper<GenericRaster>::compute_remainders(
	const std::string& semantic_id, const GenericRaster& ref_result, const PuzzleRequest &request, QueryProfiler &profiler) {
	ExecTimer t("CacheManager.puzzle.remainer_calc");

	std::vector<std::unique_ptr<GenericRaster>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	auto remainders = request.get_remainder_queries( ref_result.pixel_scale_x, ref_result.pixel_scale_y,
													 ref_result.stref.x1, ref_result.stref.y1 );

	for ( auto &rqr : remainders ) {
		try {
			auto rem = compute_item(*graph,rqr,profiler);
			if ( !CacheCommon::resolution_matches( ref_result, *rem ) ) {
				Log::warn(
					"Resolution clash on remainder. Requires: [%f,%f], result: [%f,%f], QueryRectangle: [%f,%f], %s, result-dimension: %dx%d. Fitting result!",
					ref_result.pixel_scale_x, ref_result.pixel_scale_y, rem->pixel_scale_x, rem->pixel_scale_y,
					((rqr.x2 - rqr.x1) / rqr.xres), ((rqr.y2 - rqr.y1) / rqr.yres),
					CacheCommon::qr_to_string(rqr).c_str(), rem->width, rem->height);

				rem = rem->fitToQueryRectangle(rqr);
				//throw OperatorException("Incompatible resolution on remainder");
			}
			result.push_back( std::move(rem) );
		} catch ( const MetadataException &me) {
			Log::error("Error fetching remainder: %s. Query: %s", me.what(), CacheCommon::qr_to_string(rqr).c_str());
			throw;
		} catch ( const ArgumentException &ae ) {
			Log::warn("Error fetching remainder: %s. Query: %s", ae.what(), CacheCommon::qr_to_string(rqr).c_str() );
			throw;
		}
	}
	return result;
}

RasterCacheWrapper::RasterCacheWrapper(const NodeCacheManager &mgr, NodeCache<GenericRaster>& cache, const CachingStrategy &strategy) :
		NodeCacheWrapper(mgr,cache,strategy) {
}

std::unique_ptr<GenericRaster> RasterCacheWrapper::do_puzzle(const SpatioTemporalReference &bbox,
	const std::vector<std::shared_ptr<const GenericRaster> >& items) {
	ExecTimer t("CacheManager.puzzle.do_puzzle");
	Log::trace("Puzzling raster with %d pieces", items.size() );

//	RasterWriter w = PuzzleTracer::get_writer();
//	w.write_meta(query,covered);

	// Bake result
	Log::trace("Creating result raster");

	auto &tmp = items.at(0);
	uint32_t width = std::floor((bbox.x2-bbox.x1) / tmp->pixel_scale_x);
	uint32_t height = std::floor((bbox.y2-bbox.y1) / tmp->pixel_scale_y);

	std::unique_ptr<GenericRaster> result = GenericRaster::create(
			tmp->dd,
			bbox,
			width,
			height );

	for ( auto &raster : items ) {
		auto x = result->WorldToPixelX( raster->stref.x1 );
		auto y = result->WorldToPixelY( raster->stref.y1 );

		if ( x >= width || y >= height ||
			 x + raster->width <= 0 || y + raster->height <= 0 )
			Log::info("Puzzle piece out of result-raster, result: %s, piece: %s", CacheCommon::stref_to_string(result->stref).c_str(), CacheCommon::stref_to_string(raster->stref).c_str() );
		else {
			try {
				result->blit( raster.get(), x, y );
			} catch ( const MetadataException &me ) {
				Log::error("Blit error. Result: %s, piece: %s", CacheCommon::stref_to_string(result->stref).c_str(), CacheCommon::stref_to_string(raster->stref).c_str() );
			}
		}
//		w.write_raster(*result, "dest_");
	}
	return result;


}

std::unique_ptr<GenericRaster> RasterCacheWrapper::read_item(BinaryStream &stream) {
	return GenericRaster::fromStream(stream);
}

std::unique_ptr<GenericRaster> RasterCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedRaster(query, qp );
}

PlotCacheWrapper::PlotCacheWrapper(const NodeCacheManager &mgr, NodeCache<GenericPlot>& cache, const CachingStrategy &strategy) :
	NodeCacheWrapper(mgr,cache,strategy) {
}

std::unique_ptr<GenericPlot> PlotCacheWrapper::do_puzzle(const SpatioTemporalReference &bbox,
	const std::vector<std::shared_ptr<const GenericPlot> >& items) {
	(void) bbox;
	(void) items;
	throw OperatorException("Puzzling not supported for plots");
}

std::unique_ptr<GenericPlot> PlotCacheWrapper::read_item(BinaryStream &stream) {
	return GenericPlot::fromStream(stream);
}

std::unique_ptr<GenericPlot> PlotCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedPlot(query,qp);
}

template<typename T>
FeatureCollectionCacheWrapper<T>::FeatureCollectionCacheWrapper(const NodeCacheManager &mgr, NodeCache<T>& cache, const CachingStrategy &strategy) :
	NodeCacheWrapper<T>(mgr,cache,strategy) {
}

template<typename T>
std::unique_ptr<T> FeatureCollectionCacheWrapper<T>::do_puzzle(const SpatioTemporalReference &bbox,
	const std::vector<std::shared_ptr<const T> >& items) {
	ExecTimer t("CacheManager.puzzle.do_puzzle");
	auto result = make_unique<T>(bbox);

	T& target = *result;
	target.global_attributes = items.at(0)->global_attributes;

	for ( auto &src : items ) {
		std::vector<bool> keep;
		keep.reserve( src->getFeatureCount() );

		for ( auto feature : *src ) {
			keep.push_back(
				src->featureIntersectsRectangle( feature, bbox.x1,bbox.y1,bbox.x2,bbox.y2 ) &&
				!(src->time_start[feature] > bbox.t2 || src->time_end[feature] < bbox.t1)
			);
		}
		std::unique_ptr<T> filtered = src->filter(keep);

		AttributeArraysHelper::append(target.feature_attributes, filtered->feature_attributes);

		target.coordinates.reserve(target.coordinates.size() + filtered->coordinates.size());
		target.time_start.reserve(target.time_start.size() + filtered->time_start.size());
		target.time_end.reserve(target.time_end.size() + filtered->time_end.size());

		target.coordinates.insert(target.coordinates.end(), filtered->coordinates.begin(),filtered->coordinates.end());
		target.time_start.insert(target.time_start.end(), filtered->time_start.begin(),filtered->time_start.end());
		target.time_end.insert(target.time_end.end(), filtered->time_end.begin(),filtered->time_end.end());

		append_idxs(target,*filtered);
	}
	return result;
}

template<typename T>
void FeatureCollectionCacheWrapper<T>::append_idx_vec(
		std::vector<uint32_t>& dest, const std::vector<uint32_t>& src) {
	dest.reserve( dest.size() + src.size() - 1 );
	size_t ext = dest.back();
	dest.pop_back();

	for ( auto sf : src )
		dest.push_back( sf + ext );
}

template<typename T>
void FeatureCollectionCacheWrapper<T>::combine_feature_attributes(
		AttributeArrays& dest, const AttributeArrays src) {
}

template<typename T>
std::unique_ptr<T> FeatureCollectionCacheWrapper<T>::read_item(BinaryStream &stream) {
	return make_unique<T>( stream );
}

PointCollectionCacheWrapper::PointCollectionCacheWrapper(const NodeCacheManager &mgr, NodeCache<PointCollection>& cache, const CachingStrategy &strategy) :
		FeatureCollectionCacheWrapper(mgr,cache,strategy)  {
}

std::unique_ptr<PointCollection> PointCollectionCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedPointCollection(query,qp);
}

void PointCollectionCacheWrapper::append_idxs(PointCollection& dest,
		const PointCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
}

LineCollectionCacheWrapper::LineCollectionCacheWrapper(const NodeCacheManager &mgr, NodeCache<LineCollection>& cache, const CachingStrategy &strategy) :
		FeatureCollectionCacheWrapper(mgr,cache,strategy) {
}

std::unique_ptr<LineCollection> LineCollectionCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedLineCollection(query,qp);
}

void LineCollectionCacheWrapper::append_idxs(LineCollection& dest,
		const LineCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_line, src.start_line);
}

PolygonCollectionCacheWrapper::PolygonCollectionCacheWrapper(const NodeCacheManager &mgr, NodeCache<PolygonCollection>& cache, const CachingStrategy &strategy) :
		FeatureCollectionCacheWrapper(mgr,cache,strategy) {
}

std::unique_ptr<PolygonCollection> PolygonCollectionCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedPolygonCollection(query,qp);
}

void PolygonCollectionCacheWrapper::append_idxs(PolygonCollection& dest,
		const PolygonCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_polygon, src.start_polygon);
	append_idx_vec(dest.start_ring, src.start_ring);
}

////////////////////////////////////////////////////////////
//
// MANAGER STUFF
//
////////////////////////////////////////////////////////////

thread_local BinaryStream* NodeCacheManager::index_connection = nullptr;

NodeCacheManager::NodeCacheManager( std::unique_ptr<CachingStrategy> strategy,
	size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
	size_t polygon_cache_size, size_t plot_cache_size) :
	raster_cache(CacheType::RASTER,raster_cache_size),
	point_cache(CacheType::POINT,point_cache_size),
	line_cache(CacheType::LINE,line_cache_size),
	polygon_cache(CacheType::POLYGON,polygon_cache_size),
	plot_cache(CacheType::PLOT,plot_cache_size),
	raster_wrapper(*this,raster_cache, *strategy), point_wrapper(*this,point_cache, *strategy),
	line_wrapper(*this,line_cache, *strategy), polygon_wrapper(*this,polygon_cache, *strategy),
	plot_wrapper(*this,plot_cache, *strategy),
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


	std::vector<CacheStats> stats {
		raster_cache.get_stats(),
		point_cache.get_stats(),
		line_cache.get_stats(),
		polygon_cache.get_stats(),
		plot_cache.get_stats()
	};

	return NodeStats( cap, stats );
}

void NodeCacheManager::set_index_connection(BinaryStream* con) {
	NodeCacheManager::index_connection = con;
}

BinaryStream& NodeCacheManager::get_index_connection() const {
	if ( NodeCacheManager::index_connection == nullptr )
		throw IllegalStateException("No index-connection configured for this thread");
	return *NodeCacheManager::index_connection;
}

template class NodeCacheWrapper<GenericRaster>;
template class NodeCacheWrapper<PointCollection>;
template class NodeCacheWrapper<LineCollection>;
template class NodeCacheWrapper<PolygonCollection>;
template class NodeCacheWrapper<GenericPlot>;

