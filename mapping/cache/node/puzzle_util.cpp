/*
 * puzzle_util.cpp
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */

#include "cache/node/puzzle_util.h"
#include "cache/priv/connection.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include "util/make_unique.h"

#include <limits>

/**
 * Helper class to combine attribtue arrays
 * of SimpleFeatureCollections
 */
class AttributeArraysHelper {
public:
	/**
	 * Appends the given source-array to the given destination-array
	 * @param dest the target array
	 * @param src the source array
	 */
	static void append( AttributeArrays &dest, const AttributeArrays &src );
private:
	/**
	 * Appends the given source-array to the given destination-array
	 * @param dest the target array
	 * @param src the source array
	 */
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

/////////////////////////////////////
//
// LocalRetriever
//
/////////////////////////////////////

template<class T>
LocalRetriever<T>::LocalRetriever( const NodeCache<T> &cache ) : cache(cache) {
}

template<class T>
std::shared_ptr<const T> LocalRetriever<T>::fetch( const std::string &semantic_id, const CacheRef& ref, QueryProfiler &qp) const {
	auto e = cache.get( NodeCacheKey(semantic_id, ref.entry_id) );
	qp.addTotalCosts(e->profile);
	return e->data;
}

template<class T>
std::unique_ptr<T> LocalRetriever<T>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	throw OperatorException("Computation only possible for concrete instances");
}

template<>
std::unique_ptr<GenericRaster> LocalRetriever<GenericRaster>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	auto res = op.getCachedRaster(query,qp);
	res->setRepresentation(GenericRaster::Representation::CPU);
	return res;
}

template<>
std::unique_ptr<PointCollection> LocalRetriever<PointCollection>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	return op.getCachedPointCollection(query,qp);
}

template<>
std::unique_ptr<LineCollection> LocalRetriever<LineCollection>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	return op.getCachedLineCollection(query,qp);
}

template<>
std::unique_ptr<PolygonCollection> LocalRetriever<PolygonCollection>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	return op.getCachedPolygonCollection(query,qp);
}

template<>
std::unique_ptr<GenericPlot> LocalRetriever<GenericPlot>::compute(GenericOperator& op, const QueryRectangle& query, QueryProfiler& qp) const {
	return op.getCachedPlot(query,qp);
}

/////////////////////////////////////
//
// RemoteRetriever
//
/////////////////////////////////////

template<class T>
RemoteRetriever<T>::RemoteRetriever(const NodeCache<T> &cache, const CacheRefHandler &handler) :
	LocalRetriever<T>(cache), ref_handler(handler) {
}

template<class T>
std::shared_ptr<const T> RemoteRetriever<T>::fetch(const std::string &semantic_id, const CacheRef& ref, QueryProfiler &qp) const {
	if ( ref_handler.is_local_ref(ref) ) {
		return LocalRetriever<T>::fetch(semantic_id,ref,qp);
	}
	else {
		auto res = load(semantic_id,ref,qp);
		return std::shared_ptr<const T>( res.release() );
	}
}

template<class T>
std::unique_ptr<T> RemoteRetriever<T>::load(
		const std::string& semantic_id, const CacheRef& ref, QueryProfiler& qp) const {

	TypedNodeCacheKey key( LocalRetriever<T>::cache.type, semantic_id,ref.entry_id);
	Log::debug("Fetching cache-entry from: %s:%d, key: %d", ref.host.c_str(), ref.port, ref.entry_id );

	auto con = BlockingConnection::create(ref.host,ref.port,true,DeliveryConnection::MAGIC_NUMBER);
	auto resp = con->write_and_read(DeliveryConnection::CMD_GET_CACHED_ITEM,key);
	uint8_t rc = resp->read<uint8_t>();

	switch (rc) {
		case DeliveryConnection::RESP_OK: {
			FetchInfo mi(*resp);
			// Add the original costs
			qp.addTotalCosts(mi.profile);
			// Add the network-costs
			qp.addIOCost( mi.size );
			return read_item(*resp);
		}
		case DeliveryConnection::RESP_ERROR: {
			std::string err_msg = resp->read<std::string>();
			Log::error("Delivery returned error: %s", err_msg.c_str());
			throw DeliveryException(err_msg);
		}
		default: {
			Log::error("Delivery returned unknown code: %d", rc);
			throw DeliveryException("Delivery returned unknown code");
		}
	}
}


template<class T>
std::unique_ptr<T> RemoteRetriever<T>::read_item(BinaryReadBuffer& buffer) const {
	return make_unique<T>(buffer);
}

template<>
std::unique_ptr<GenericRaster> RemoteRetriever<GenericRaster>::read_item(BinaryReadBuffer& buffer) const {
	return GenericRaster::deserialize(buffer);
}

template<>
std::unique_ptr<GenericPlot> RemoteRetriever<GenericPlot>::read_item(BinaryReadBuffer& buffer) const {
	return GenericPlot::deserialize(buffer);
}

/////////////////////////////////////
//
// Puzzler
//
/////////////////////////////////////


std::unique_ptr<GenericPlot> PlotPuzzler::puzzle(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const GenericPlot> >& items) const {
	(void) bbox;
	(void) items;
	throw OperatorException("Puzzling not supported for plots");
}

std::unique_ptr<GenericRaster> RasterPuzzler::puzzle(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const GenericRaster> >& items) const {

	TIME_EXEC("Puzzler.puzzle");
	Log::trace("Puzzling raster with %d pieces", items.size() );

	auto &tmp = items.at(0);
	uint32_t width = std::floor((bbox.x2-bbox.x1) / tmp->pixel_scale_x);
	uint32_t height = std::floor((bbox.y2-bbox.y1) / tmp->pixel_scale_y);

	std::unique_ptr<GenericRaster> result = GenericRaster::create(
			tmp->dd,
			bbox,
			width,
			height );

	result->global_attributes = items[0]->global_attributes;

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
				Log::error("Blit error: %s\nResult: %s\npiece : %s", me.what(), CacheCommon::stref_to_string(result->stref).c_str(), CacheCommon::stref_to_string(raster->stref).c_str() );
			}
		}
	}
	return result;
}

template<class T>
std::unique_ptr<T> SimpleFeaturePuzzler<T>::puzzle(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const T> >& items) const {

	TIME_EXEC("Puzzler.puzzle");
	auto result = make_unique<T>(bbox);

	T& target = *result;
	target.global_attributes = items.at(0)->global_attributes;

	for ( auto &src : items ) {
		std::vector<bool> keep;
		keep.reserve( src->getFeatureCount() );

		for ( auto feature : *src ) {
			keep.push_back(
				src->featureIntersectsRectangle( feature, bbox.x1,bbox.y1,bbox.x2,bbox.y2 ) &&
				!(src->time[feature].t1 > bbox.t2 || src->time[feature].t2 < bbox.t1)
			);
		}
		std::unique_ptr<T> filtered = src->filter(keep);

		AttributeArraysHelper::append(target.feature_attributes, filtered->feature_attributes);

		target.coordinates.reserve(target.coordinates.size() + filtered->coordinates.size());
		target.time.reserve(target.time.size() + filtered->time.size());

		target.coordinates.insert(target.coordinates.end(), filtered->coordinates.begin(),filtered->coordinates.end());
		target.time.insert(target.time.end(), filtered->time.begin(),filtered->time.end());

		append_idxs(target,*filtered);
	}
	return result;
}

template<class T>
void SimpleFeaturePuzzler<T>::append_idx_vec(std::vector<uint32_t>& dest,
		const std::vector<uint32_t>& src) const {
	dest.reserve( dest.size() + src.size() - 1 );
	size_t ext = dest.back();
	dest.pop_back();

	for ( auto sf : src )
		dest.push_back( sf + ext );
}

void PointCollectionPuzzler::append_idxs(PointCollection& dest,
		const PointCollection& src) const {
	append_idx_vec(dest.start_feature, src.start_feature);
}

void LineCollectionPuzzler::append_idxs(LineCollection& dest,
		const LineCollection& src) const {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_line, src.start_line);
}

void PolygonCollectionPuzzler::append_idxs(PolygonCollection& dest,
		const PolygonCollection& src) const {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_polygon, src.start_polygon);
	append_idx_vec(dest.start_ring, src.start_ring);
}


///////////////////////////////////////////////////////////////////////
//
// PUZZLE UTIL
//
///////////////////////////////////////////////////////////////////////


template<class T>
PuzzleUtil<T>::PuzzleUtil(
		const PieceRetriever<T> &retriever,
		std::unique_ptr<Puzzler<T> > puzzler) : retriever(retriever), puzzler(std::move(puzzler)) {
}

template<class T>
std::unique_ptr<T> PuzzleUtil<T>::process_puzzle(const PuzzleRequest& request, QueryProfiler& qp) const {
	TIME_EXEC("PuzzleUtil.process_puzzle");
	Log::trace("Processing puzzle-request: %s", request.to_string().c_str());

	std::vector<std::shared_ptr<const T>> items;
	items.reserve( request.parts.size() + request.get_num_remainders() );
	{
		TIME_EXEC("PuzzleUtil.puzzle.fetch_parts");
		// Fetch puzzle parts
		Log::trace("Fetching all puzzle-parts");
		for (const CacheRef &cr : request.parts) {
			items.push_back(retriever.fetch( request.semantic_id, cr, qp ));
		}
	}

	// Create remainder
	Log::trace("Creating remainder queries.");
	auto ref = items.front();
	auto remainders = compute_remainders(request.semantic_id,*ref,request, qp);

	for ( auto &r : remainders )
		items.push_back( std::shared_ptr<T>(r.release()));

	auto bounds = enlarge_puzzle(request.query,items);
	auto result = puzzler->puzzle( bounds, items );
	Log::trace("Finished processing puzzle-request: %s", request.to_string().c_str());
	return result;
}


template<class T>
std::vector<std::unique_ptr<T> > PuzzleUtil<T>::compute_remainders(
		const std::string& semantic_id, const T& ref_result,
		const PuzzleRequest& request, QueryProfiler& profiler) const {
	TIME_EXEC("PuzzleUtil.compute_remainders");
	std::vector<std::unique_ptr<T>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	{
		QueryProfilerStoppingGuard sg(profiler);
		for ( auto &rqr : request.get_remainder_queries(ref_result) ) {
			result.push_back( retriever.compute(*graph,rqr,profiler) );
		}
	}
	return result;
}

template<>
std::vector<std::unique_ptr<GenericRaster>> PuzzleUtil<GenericRaster>::compute_remainders(
	const std::string& semantic_id, const GenericRaster& ref_result,
	const PuzzleRequest &request, QueryProfiler &profiler) const {
	TIME_EXEC("PuzzleUtil.compute_remainders");

	std::vector<std::unique_ptr<GenericRaster>> result;
	auto graph = GenericOperator::fromJSON(semantic_id);

	auto remainders = request.get_remainder_queries( ref_result );

	for ( auto &rqr : remainders ) {
		try {
			std::unique_ptr<GenericRaster> rem;
			{
				QueryProfilerStoppingGuard sg(profiler);
				rem = retriever.compute(*graph,rqr,profiler);
			}
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

template<class T>
SpatioTemporalReference PuzzleUtil<T>::enlarge_puzzle(
		const QueryRectangle& query,
		const std::vector<std::shared_ptr<const T> >& items) const {

	TIME_EXEC("PuzzleUtil.enlarge");

	// Calculated maximum covered rectangle
	double values[6] = { -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
						 -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
						 -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity() };
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

template<>
SpatioTemporalReference PuzzleUtil<GenericPlot>::enlarge_puzzle(const QueryRectangle& query,
	const std::vector<std::shared_ptr<const GenericPlot> >& items) const {
	(void) items;
	return SpatioTemporalReference(query,query);
}

//
// INSTANTIATE ALL
//

template class LocalRetriever<GenericRaster>;
template class LocalRetriever<GenericPlot>;
template class LocalRetriever<PointCollection>;
template class LocalRetriever<LineCollection>;
template class LocalRetriever<PolygonCollection>;

template class RemoteRetriever<GenericRaster>;
template class RemoteRetriever<GenericPlot>;
template class RemoteRetriever<PointCollection>;
template class RemoteRetriever<LineCollection>;
template class RemoteRetriever<PolygonCollection>;

template class Puzzler<GenericRaster>;
template class Puzzler<GenericPlot>;
template class Puzzler<PointCollection>;
template class Puzzler<LineCollection>;
template class Puzzler<PolygonCollection>;

template class SimpleFeaturePuzzler<PointCollection>;
template class SimpleFeaturePuzzler<LineCollection>;
template class SimpleFeaturePuzzler<PolygonCollection>;

template class PuzzleUtil<GenericRaster>;
template class PuzzleUtil<GenericPlot>;
template class PuzzleUtil<PointCollection>;
template class PuzzleUtil<LineCollection>;
template class PuzzleUtil<PolygonCollection>;

