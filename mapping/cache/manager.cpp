/*
 * cache_manager.cpp
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/manager.h"
#include "cache/priv/connection.h"
#include "node/puzzletracer.h"
#include "util/log.h"

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
// NOP-Wrapper
//

template<typename T>
NopCacheWrapper<T>::NopCacheWrapper() {
}

template<typename T>
void NopCacheWrapper<T>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item) {
	(void) semantic_id;
	(void) item;
}

template<typename T>
std::unique_ptr<T> NopCacheWrapper<T>::query(const GenericOperator& op,
		const QueryRectangle& rect) {
	(void) op;
	(void) rect;
	throw NoSuchElementException("NOP-Cache has no entries");
}

template<typename T>
NodeCacheRef NopCacheWrapper<T>::put_local(
		const std::string& semantic_id, const std::unique_ptr<T>& item,
		const AccessInfo info) {
	CacheEntry ce( CacheCube(*item), sizeof(T), info.last_access, info.access_count );
	return NodeCacheRef( CacheType::UNKNOWN, semantic_id, 0, ce );
}

template<typename T>
void NopCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	(void) key;
}

template<typename T>
const std::shared_ptr<const T> NopCacheWrapper<T>::get_ref(
		const NodeCacheKey& key) {
	(void) key;
	throw NoSuchElementException("NOP-Cache has no entries");
}

template<typename T>
NodeCacheRef NopCacheWrapper<T>::get_entry_info(
		const NodeCacheKey& key) {
	(void) key;
	throw NoSuchElementException("NOP-Cache has no entries");
}

template<typename T>
std::unique_ptr<T> NopCacheWrapper<T>::process_puzzle(
		const PuzzleRequest& request) {
	(void) request;
	throw NoSuchElementException("NOP-Cache has no entries");
}

//
// Remote Wrapper
//


template<typename T>
RemoteCacheWrapper<T>::RemoteCacheWrapper(NodeCache<T>& cache, const std::string &my_host, int my_port) :
	cache(cache), my_host(my_host), my_port(my_port) {
}

template<typename T>
void RemoteCacheWrapper<T>::put(const std::string& semantic_id, const std::unique_ptr<T>& item) {
	if (CacheManager::remote_connection == nullptr)
		throw NetworkException("No connection to remote-index.");

	auto ref = put_local(semantic_id, item);
	BinaryStream &stream = *CacheManager::remote_connection;

	Log::debug("Adding item to remote cache: %s", ref.to_string().c_str());
	stream.write(WorkerConnection::RESP_NEW_CACHE_ENTRY);
	ref.toStream(stream);
}

template<typename T>
NodeCacheRef RemoteCacheWrapper<T>::put_local(const std::string& semantic_id,
	const std::unique_ptr<T>& item, const AccessInfo info) {
	Log::debug("Adding item to local cache");
	return cache.put(semantic_id, item, info);
}

template<typename T>
void RemoteCacheWrapper<T>::remove_local(const NodeCacheKey& key) {
	Log::debug("Removing item from local cache. Key: %s", key.to_string().c_str());
	cache.remove(key);
}

template<typename T>
const std::shared_ptr<const T> RemoteCacheWrapper<T>::get_ref(const NodeCacheKey& key) {
	Log::debug("Getting item from local cache. Key: %s", key.to_string().c_str());
	return cache.get(key);
}

template<typename T>
NodeCacheRef RemoteCacheWrapper<T>::get_entry_info(const NodeCacheKey& key) {
	return cache.get_entry_metadata(key);
}

template<typename T>
std::unique_ptr<T> RemoteCacheWrapper<T>::query(const GenericOperator& op, const QueryRectangle& rect) {
	Log::debug("Querying item: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	// Local lookup
	CacheQueryResult<uint64_t> qres = cache.query(op.getSemanticId(), rect);

	Log::debug("QueryResult: %s", qres.to_string().c_str() );

	// Full single local hit
	if ( !qres.has_remainder() && qres.keys.size() == 1 ) {
		Log::trace("Full single local HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		NodeCacheKey key(op.getSemanticId(), qres.keys.at(0));
		return cache.get_copy(key);
	}
	// Full local hit (puzzle)
	else if ( !qres.has_remainder() ) {
		Log::trace("Full local HIT for query: %s on %s. Puzzling result.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		std::vector<CacheRef> refs;
		for ( auto &id : qres.keys )
			refs.push_back( CacheRef(my_host,my_port,id) );

		PuzzleRequest pr( cache.type, op.getSemanticId(), rect, qres.remainder, refs );
		std::unique_ptr<T> result = process_puzzle(pr);
		put( op.getSemanticId(), result );
		return result;
	}
	// Check index (if we aren't on depth = 0 )
	else if ( op.getDepth() != 0 ) {
		Log::debug("Local MISS for query: %s on %s. Querying index.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		BaseRequest cr(CacheType::RASTER, op.getSemanticId(), rect);
		BinaryStream &stream = *CacheManager::remote_connection;

		stream.write(WorkerConnection::CMD_QUERY_CACHE);
		cr.toStream(stream);

		uint8_t resp;
		stream.read(&resp);
		switch (resp) {
			// Full hit on different client
			case WorkerConnection::RESP_QUERY_HIT: {
				Log::trace("Full single remote HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
				return fetch_item( op.getSemanticId(), CacheRef(stream) );
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
				auto res = process_puzzle(pr);
				put(op.getSemanticId(),res);
				return res;
				break;
			}
			default: {
				throw NetworkException("Received unknown response from index.");
			}
		}
	}
	else {
		Log::debug("Graph-Depth = 0, omitting index query, returning MISS.");
		throw NoSuchElementException("Cache-Miss");
	}
}

template<typename T>
std::unique_ptr<T> RemoteCacheWrapper<T>::process_puzzle(const PuzzleRequest& request) {
	Log::trace("Processing puzzle-request: %s", request.to_string().c_str());

	std::vector<std::shared_ptr<const T>> items;


	// Fetch puzzle parts
	Log::trace("Fetching all puzzle-parts");
	for (const CacheRef &cr : request.parts) {
		if (cr.host == my_host && cr.port == my_port) {
			Log::trace("Fetching puzzle-piece from local cache, key: %d", cr.entry_id);
			items.push_back(get_ref( NodeCacheKey(request.semantic_id, cr.entry_id) ));
		}
		else {
			Log::debug("Fetching puzzle-piece from %s:%d, key: %d", cr.host.c_str(), cr.port, cr.entry_id);
			auto raster = fetch_item( request.semantic_id, cr );
			items.push_back( std::shared_ptr<const T>(raster.release()) );
		}
	}

	// Create remainder
	Log::trace("Creating remainder queries.");
	auto ref = items.front();
	auto remainders = compute_remainders(request.semantic_id,*ref,request);

	for ( auto &r : remainders )
		items.push_back( std::shared_ptr<T>(r.release()));

	auto result = do_puzzle( enlarge_puzzle(request.query,items), items);
	Log::trace("Finished processing puzzle-request: %s", request.to_string().c_str());
	return result;
}

template<typename T>
SpatioTemporalReference RemoteCacheWrapper<T>::enlarge_puzzle(const QueryRectangle& query,
	const std::vector<std::shared_ptr<const T> >& items) {
	// TODO
	(void) items;
	return SpatioTemporalReference(query,query);
}

template<typename T>
std::unique_ptr<T> RemoteCacheWrapper<T>::fetch_item(const std::string& semantic_id, const CacheRef& ref) {
	TypedNodeCacheKey key(cache.type,semantic_id,ref.entry_id);
	Log::debug("Fetching cache-entry from: %s:%d, key: %d", ref.host.c_str(), ref.port, ref.entry_id );
	UnixSocket sock(ref.host.c_str(), ref.port);

	BinaryStream &stream = sock;
	stream.write(DeliveryConnection::MAGIC_NUMBER);
	stream.write(DeliveryConnection::CMD_GET_CACHED_ITEM);
	key.toStream(stream);

	uint8_t resp;
	stream.read(&resp);
	switch (resp) {
		case DeliveryConnection::RESP_OK: {
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
std::vector<std::unique_ptr<T>> RemoteCacheWrapper<T>::compute_remainders(const std::string& semantic_id,
	const T& ref_result, const PuzzleRequest &request ) {
	(void) ref_result;
	std::vector<std::unique_ptr<T>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	for ( auto &rqr : request.get_remainder_queries() ) {
		QueryProfiler qp;
		result.push_back( compute_item(*graph,rqr,qp) );
	}
	return result;
}

template<>
std::vector<std::unique_ptr<GenericRaster>> RemoteCacheWrapper<GenericRaster>::compute_remainders(
	const std::string& semantic_id, const GenericRaster& ref_result, const PuzzleRequest &request) {

	std::vector<std::unique_ptr<GenericRaster>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	auto remainders = request.get_remainder_queries( ref_result.pixel_scale_x, ref_result.pixel_scale_y,
													 ref_result.stref.x1, ref_result.stref.y1 );

	for ( auto &rqr : remainders ) {
		try {
			QueryProfiler qp;
			qp.startTimer();
			// FIXME: Do sth. on rasterdb to make this work with RasterQM::LOOSE
			auto rem = compute_item(*graph,rqr,qp);

			if ( std::abs(1.0 - ref_result.pixel_scale_x / rem->pixel_scale_x) > 0.01 ||
				 std::abs(1.0 - ref_result.pixel_scale_y / rem->pixel_scale_y) > 0.01 ) {
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

RasterCacheWrapper::RasterCacheWrapper(NodeCache<GenericRaster>& cache, const std::string& my_host,
	int my_port) : RemoteCacheWrapper(cache,my_host,my_port) {
}

std::unique_ptr<GenericRaster> RasterCacheWrapper::do_puzzle(const SpatioTemporalReference &bbox,
	const std::vector<std::shared_ptr<const GenericRaster> >& items) {
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

PlotCacheWrapper::PlotCacheWrapper(NodeCache<GenericPlot>& cache, const std::string& my_host, int my_port) :
	RemoteCacheWrapper(cache,my_host,my_port) {
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
FeatureCollectionCacheWrapper<T>::FeatureCollectionCacheWrapper(NodeCache<T>& cache,
	const std::string& my_host, int my_port) : RemoteCacheWrapper<T>(cache,my_host,my_port) {
}

template<typename T>
std::unique_ptr<T> FeatureCollectionCacheWrapper<T>::do_puzzle(const SpatioTemporalReference &bbox,
	const std::vector<std::shared_ptr<const T> >& items) {
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

PointCollectionCacheWrapper::PointCollectionCacheWrapper(NodeCache<PointCollection>& cache,
	const std::string& my_host, int my_port) : FeatureCollectionCacheWrapper(cache,my_host,my_port)  {
}

std::unique_ptr<PointCollection> PointCollectionCacheWrapper::compute_item(GenericOperator& op,
	const QueryRectangle& query, QueryProfiler &qp) {
	return op.getCachedPointCollection(query,qp);
}

void PointCollectionCacheWrapper::append_idxs(PointCollection& dest,
		const PointCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
}

LineCollectionCacheWrapper::LineCollectionCacheWrapper(NodeCache<LineCollection>& cache,
	const std::string& my_host, int my_port) : FeatureCollectionCacheWrapper(cache,my_host,my_port) {
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

PolygonCollectionCacheWrapper::PolygonCollectionCacheWrapper(NodeCache<PolygonCollection>& cache,
	const std::string& my_host, int my_port) : FeatureCollectionCacheWrapper(cache,my_host,my_port) {
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

//
// Cache-Manager
//
std::unique_ptr<CachingStrategy> CacheManager::strategy;
std::unique_ptr<CacheManager> CacheManager::instance;
thread_local UnixSocket* CacheManager::remote_connection = nullptr;

CachingStrategy& CacheManager::get_strategy() {
	if (CacheManager::strategy)
		return *strategy;
	else
		throw NotInitializedException(
			"CacheManager was not initialized. Please use CacheManager::init first.");
}

CacheManager& CacheManager::get_instance() {
	if ( CacheManager::instance )
		return *instance;
	else
		throw NotInitializedException(
			"CacheManager was not initialized. Please use CacheManager::init first.");
}

void CacheManager::init(std::unique_ptr<CacheManager> instance,std::unique_ptr<CachingStrategy> strategy) {
	CacheManager::strategy.reset(strategy.release());
	CacheManager::instance.reset(instance.release());
}


//
// Default Manager
//


DefaultCacheManager::DefaultCacheManager(const std::string &my_host, int my_port,
	size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
	size_t polygon_cache_size, size_t plot_cache_size) :
	my_host(my_host), my_port(my_port),
	raster_cache(raster_cache_size), point_cache(point_cache_size),
	line_cache(line_cache_size), polygon_cache(polygon_cache_size),
	plot_cache(plot_cache_size),
	raster_wrapper(raster_cache, my_host, my_port), point_wrapper(point_cache, my_host, my_port),
	line_wrapper(line_cache, my_host, my_port), polygon_wrapper(polygon_cache, my_host, my_port),
	plot_wrapper(plot_cache, my_host, my_port) {
}

NodeHandshake DefaultCacheManager::get_handshake() const {

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

	return NodeHandshake(my_host, my_port, cap, entries );
}

NodeStats DefaultCacheManager::get_stats() const {

	Capacity cap(
		raster_cache.get_max_size(), raster_cache.get_current_size(),
		point_cache.get_max_size(), point_cache.get_current_size(),
		line_cache.get_max_size(), line_cache.get_current_size(),
		polygon_cache.get_max_size(), polygon_cache.get_current_size(),
		plot_cache.get_max_size(), plot_cache.get_current_size()
	);

	std::vector<CacheStats> stats{
		raster_cache.get_stats(),
		point_cache.get_stats(),
		line_cache.get_stats(),
		polygon_cache.get_stats(),
		plot_cache.get_stats(),
	};

	return NodeStats( cap, stats );
}

CacheWrapper<GenericRaster>& DefaultCacheManager::get_raster_cache() {
	return raster_wrapper;
}

CacheWrapper<PointCollection>& DefaultCacheManager::get_point_cache() {
	return point_wrapper;
}

CacheWrapper<LineCollection>& DefaultCacheManager::get_line_cache() {
	return line_wrapper;
}

CacheWrapper<PolygonCollection>& DefaultCacheManager::get_polygon_cache() {
	return polygon_wrapper;
}

CacheWrapper<GenericPlot>& DefaultCacheManager::get_plot_cache() {
	return plot_wrapper;
}

//
// NOP-Cache
//

NopCacheManager::NopCacheManager(const std::string &my_host, int my_port) :
	my_host(my_host), my_port(my_port) {
}

NodeHandshake NopCacheManager::get_handshake() const {
	return NodeHandshake(my_host, my_port, Capacity(0,0,0,0,0,0,0,0,0,0), std::vector<NodeCacheRef>() );
}

NodeStats NopCacheManager::get_stats() const {
	return NodeStats(Capacity(0,0,0,0,0,0,0,0,0,0), std::vector<CacheStats>() );
}

CacheWrapper<GenericRaster>& NopCacheManager::get_raster_cache() {
	return raster_cache;
}

CacheWrapper<PointCollection>& NopCacheManager::get_point_cache() {
	return point_cache;
}

CacheWrapper<LineCollection>& NopCacheManager::get_line_cache() {
	return line_cache;
}

CacheWrapper<PolygonCollection>& NopCacheManager::get_polygon_cache() {
	return poly_cache;
}

CacheWrapper<GenericPlot>& NopCacheManager::get_plot_cache() {
	return plot_cache;
}
