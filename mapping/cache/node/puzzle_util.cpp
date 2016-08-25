/*
 * puzzle_util.cpp
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */

#include "cache/node/puzzle_util.h"
#include "cache/priv/connection.h"
#include "cache/node/nodeserver.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include "util/make_unique.h"
#include "util/log.h"

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
	static void append(AttributeArrays &dest, const AttributeArrays &src);
private:
	/**
	 * Appends the given source-array to the given destination-array
	 * @param dest the target array
	 * @param src the source array
	 */
	template<typename T>
	static void append_arr(AttributeArrays::AttributeArray<T> &dest,
			const AttributeArrays::AttributeArray<T> &src);
};

void AttributeArraysHelper::append(AttributeArrays &dest,
		const AttributeArrays &src) {
	for (auto &n : dest._numeric) {
		append_arr(n.second, src._numeric.at(n.first));
	}
	for (auto &n : dest._textual) {
		append_arr(n.second, src._textual.at(n.first));
	}
}

template<typename T>
void AttributeArraysHelper::append_arr(AttributeArrays::AttributeArray<T>& dest,
		const AttributeArrays::AttributeArray<T>& src) {
	dest.reserve(dest.array.size() + src.array.size());
	dest.array.insert(dest.array.end(), src.array.begin(), src.array.end());
}

template<class T>
std::unique_ptr<T> PuzzleUtil::process(GenericOperator &op,
		const QueryRectangle& query, const std::vector<Cube<3> >& remainder,
		const std::vector<std::shared_ptr<const T> >& items,
		QueryProfiler &profiler) {

	TIME_EXEC("PuzzleUtil.process_puzzle");
	Log::trace("Processing puzzle-request with %ld available items and %ld remainders: %s", items.size(), remainder.size());

	// Create remainder
	Log::trace("Creating remainder queries.");

	auto ref = items.front();
	auto remainders = compute_remainders<T>(query, op, *ref, remainder, profiler);

	std::vector<std::shared_ptr<const T>> all_items = items;

	for (auto &r : remainders)
		all_items.push_back(std::shared_ptr<const T>(r.release()));

	auto bounds = enlarge_puzzle(query, all_items);
	auto result = puzzle(bounds, all_items);
	Log::trace("Finished processing puzzle-request:");
	return result;
}

template<class T>
std::vector<std::unique_ptr<T> > PuzzleUtil::compute_remainders(
		const QueryRectangle& query, GenericOperator &op, const T& ref_result, const std::vector<Cube<3> >& remainder, QueryProfiler& profiler) {
	TIME_EXEC("PuzzleUtil.compute_remainders");
	std::vector<std::unique_ptr<T>> result;
	{
		QueryProfilerStoppingGuard sg(profiler);
		auto rem_queries = get_remainder_queries(query, remainder, ref_result);
		for (auto &rqr : rem_queries) {
			result.push_back(compute<T>(op, rqr, profiler));
		}
	}
	return result;
}

template<class T>
std::vector<QueryRectangle> PuzzleUtil::get_remainder_queries(
		const QueryRectangle& query, const std::vector<Cube<3> >& remainder, const T& ref_result) {
	(void) ref_result;
	std::vector<QueryRectangle> result;
	result.reserve(remainder.size());

	for ( auto &rem : remainder ) {
		result.push_back( QueryRectangle( SpatialReference(query.epsg, rem.get_dimension(0).a, rem.get_dimension(1).
												a, rem.get_dimension(0).b, rem.get_dimension(1).b),
											TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
											QueryResolution::none() ) );
	}
	return result;
}

template<>
std::vector<QueryRectangle> PuzzleUtil::get_remainder_queries(
		const QueryRectangle& query, const std::vector<Cube<3> >& remainder, const GenericRaster& ref_result) {

	std::vector<QueryRectangle> result;
	result.reserve(remainder.size());

	for ( auto &rem : remainder ) {
		double x1 = rem.get_dimension(0).a;
		double x2 = rem.get_dimension(0).b;
		double y1 = rem.get_dimension(1).a;
		double y2 = rem.get_dimension(1).b;


		// Skip useless remainders
		if ( rem.get_dimension(0).distance() < ref_result.pixel_scale_x / 2 ||
			 rem.get_dimension(1).distance() < ref_result.pixel_scale_y / 2)
			continue;
		// Make sure we have at least one pixel
		snap_to_pixel_grid(x1,x2,ref_result.stref.x1,ref_result.pixel_scale_x);
		snap_to_pixel_grid(y1,y2,ref_result.stref.y1,ref_result.pixel_scale_y);


		result.push_back( QueryRectangle( SpatialReference(query.epsg, x1,y1,x2,y2),
										  TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
										  QueryResolution::pixels( std::round((x2-x1) / ref_result.pixel_scale_x),
																   std::round((y2-y1) / ref_result.pixel_scale_y) ) ) );
	}
	return result;
}


void PuzzleUtil::snap_to_pixel_grid( double &v1, double &v2, double ref, double scale ) {
	if ( ref < v1 )
		v1 = ref + std::floor( (v1-ref) / scale)*scale;
	else
		v1 = ref - std::ceil( (ref-v1) / scale)*scale;
	v2 = v1 + std::ceil( (v2-v1) / scale)*scale;
}

//
// enlarge
//

template<class T>
SpatioTemporalReference PuzzleUtil::enlarge_puzzle(const QueryRectangle& query,
		const std::vector<std::shared_ptr<const T> >& items) {

	TIME_EXEC("PuzzleUtil.enlarge");

	// Calculated maximum covered rectangle
	double values[6] = { -std::numeric_limits<double>::infinity(),
			std::numeric_limits<double>::infinity(),
			-std::numeric_limits<double>::infinity(),
			std::numeric_limits<double>::infinity(),
			-std::numeric_limits<double>::infinity(),
			std::numeric_limits<double>::infinity() };
	QueryCube qc(query);
	for (auto &item : items) {
		Cube3 ic(item->stref.x1, item->stref.x2, item->stref.y1, item->stref.y2,
				item->stref.t1, item->stref.t2);

		for (int i = 0; i < 3; i++) {
			auto &cdim = ic.get_dimension(i);
			auto &qdim = qc.get_dimension(i);
			int idx_l = 2 * i;
			int idx_r = idx_l + 1;

			// If this item touches the bounds of the query.... extend
			if (cdim.a <= qdim.a)
				values[idx_l] = std::max(values[idx_l], cdim.a);

			if (cdim.b >= qdim.b)
				values[idx_r] = std::min(values[idx_r], cdim.b);
		}
	}

	// Final check... stupid floating point stuff
	for (int i = 0; i < 6; i++) {
		if (!std::isfinite(values[i])) {
			auto &d = qc.get_dimension(i / 2);
			values[i] = (i % 2) == 0 ? d.a : d.b;
		}
	}

	return SpatioTemporalReference(
			SpatialReference(query.epsg, values[0], values[2], values[1],
					values[3]),
			TemporalReference(query.timetype, values[4], values[5]));
}

template<>
SpatioTemporalReference PuzzleUtil::enlarge_puzzle(const QueryRectangle& query,
		const std::vector<std::shared_ptr<const GenericPlot> >& items) {
	(void) items;
	return SpatioTemporalReference(query, query);
}

//
// Compute
//

template<class T>
std::unique_ptr<T> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	throw OperatorException("Computation only possible for concrete instances");
}

template<>
std::unique_ptr<GenericRaster> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	auto res = op.getCachedRaster(query, qp);
	res->setRepresentation(GenericRaster::Representation::CPU);
	return res;
}

template<>
std::unique_ptr<PointCollection> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	return op.getCachedPointCollection(query, qp);
}

template<>
std::unique_ptr<LineCollection> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	return op.getCachedLineCollection(query, qp);
}

template<>
std::unique_ptr<PolygonCollection> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	return op.getCachedPolygonCollection(query, qp);
}

template<>
std::unique_ptr<GenericPlot> PuzzleUtil::compute(GenericOperator& op,
		const QueryRectangle& query, QueryProfiler& qp) {
	return op.getCachedPlot(query, qp);
}

//
// Puzzle
//

template<class T>
std::unique_ptr<T> PuzzleUtil::puzzle(const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const T> >& items) {
	(void) bbox;
	(void) items;
	throw OperatorException("Puzzling only possible for concrete instances");
}

template<>
std::unique_ptr<PointCollection> PuzzleUtil::puzzle(const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const PointCollection> >& items) {
	return puzzle_feature_collection(bbox,items);
}

template<>
std::unique_ptr<LineCollection> PuzzleUtil::puzzle(const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const LineCollection> >& items) {
	return puzzle_feature_collection(bbox,items);
}

template<>
std::unique_ptr<PolygonCollection> PuzzleUtil::puzzle(const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const PolygonCollection> >& items) {
	return puzzle_feature_collection(bbox,items);
}

template<>
std::unique_ptr<GenericPlot> PuzzleUtil::puzzle(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const GenericPlot> >& items) {
	(void) bbox;
	(void) items;
	throw OperatorException("Puzzling not supported for plots");
}

template<>
std::unique_ptr<GenericRaster> PuzzleUtil::puzzle(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const GenericRaster> >& items) {
	TIME_EXEC("Puzzler.puzzle");
	Log::trace("Puzzling raster with %d pieces", items.size());

	auto &tmp = items.at(0);
	uint32_t width = std::floor((bbox.x2 - bbox.x1) / tmp->pixel_scale_x);
	uint32_t height = std::floor((bbox.y2 - bbox.y1) / tmp->pixel_scale_y);

	std::unique_ptr<GenericRaster> result = GenericRaster::create(tmp->dd, bbox,
			width, height);

	result->global_attributes = items[0]->global_attributes;

	for (auto &raster : items) {
		auto x = result->WorldToPixelX(raster->stref.x1);
		auto y = result->WorldToPixelY(raster->stref.y1);

		if (x >= width || y >= height || x + raster->width <= 0
				|| y + raster->height <= 0)
			Log::debug(
					"Puzzle piece out of result-raster, target: pos[%dx%d] dim[%dx%d], piece: dim[%dx%d]", x, y, result->width, result->height, raster->width, raster->height);
		else {
			try {
				result->blit(raster.get(), x, y);
			} catch (const MetadataException &me) {
				Log::error("Blit error: %s\nResult: %s\npiece : %s", me.what(),
						CacheCommon::stref_to_string(result->stref).c_str(),
						CacheCommon::stref_to_string(raster->stref).c_str());
			}
		}
	}
	return result;
}

//
// Feature collections
//

template<class T>
std::unique_ptr<T> PuzzleUtil::puzzle_feature_collection(
		const SpatioTemporalReference& bbox,
		const std::vector<std::shared_ptr<const T> >& items) {
	TIME_EXEC("Puzzler.puzzle");
	auto result = make_unique<T>(bbox);

	T& target = *result;
	target.global_attributes = items.at(0)->global_attributes;

	for (auto &src : items) {
		std::vector<bool> keep;
		keep.reserve(src->getFeatureCount());

		for (auto feature : *src) {
			keep.push_back(
					src->featureIntersectsRectangle(feature, bbox.x1, bbox.y1,
							bbox.x2, bbox.y2) && (!src->hasTime() ||
							 !(src->time[feature].t1 > bbox.t2
									|| src->time[feature].t2 < bbox.t1)));
		}
		std::unique_ptr<T> filtered = src->filter(keep);

		AttributeArraysHelper::append(target.feature_attributes,
				filtered->feature_attributes);

		target.coordinates.reserve(
				target.coordinates.size() + filtered->coordinates.size());
		target.time.reserve(target.time.size() + filtered->time.size());

		target.coordinates.insert(target.coordinates.end(),
				filtered->coordinates.begin(), filtered->coordinates.end());
		target.time.insert(target.time.end(), filtered->time.begin(),
				filtered->time.end());

		append_idxs<T>(target, *filtered);
	}
	return result;
}

template<>
void PuzzleUtil::append_idxs(PointCollection& dest, const PointCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
}

template<>
void PuzzleUtil::append_idxs(LineCollection& dest, const LineCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_line, src.start_line);
}

template<>
void PuzzleUtil::append_idxs(PolygonCollection& dest, const PolygonCollection& src) {
	append_idx_vec(dest.start_feature, src.start_feature);
	append_idx_vec(dest.start_polygon, src.start_polygon);
	append_idx_vec(dest.start_ring, src.start_ring);
}

void PuzzleUtil::append_idx_vec(std::vector<uint32_t>& dest,
		const std::vector<uint32_t>& src) {
	dest.reserve(dest.size() + src.size() - 1);
	size_t ext = dest.back();
	dest.pop_back();

	for (auto sf : src)
		dest.push_back(sf + ext);
}

/////////////////////////////////////
//
// LocalRetriever
//
/////////////////////////////////////

template<class T>
LocalRetriever<T>::LocalRetriever(const NodeCacheWrapper<T> &cache) :
		cache(cache) {
}

template<class T>
std::shared_ptr<const T> LocalRetriever<T>::fetch(
		const std::string &semantic_id, const CacheRef& ref,
		QueryProfiler &qp) const {
	auto e = cache.get(NodeCacheKey(semantic_id, ref.entry_id));
	qp.addTotalCosts(e->profile);
	return e->data;
}

/////////////////////////////////////
//
// RemoteRetriever
//
/////////////////////////////////////

template<class T>
RemoteRetriever<T>::RemoteRetriever(const NodeCacheWrapper<T> &cache,
		const CacheRefHandler &handler) :
		LocalRetriever<T>(cache), ref_handler(handler) {
}

template<class T>
std::shared_ptr<const T> RemoteRetriever<T>::fetch(
		const std::string &semantic_id, const CacheRef& ref,
		QueryProfiler &qp) const {
	if (ref_handler.is_local_ref(ref)) {
		return LocalRetriever<T>::fetch(semantic_id, ref, qp);
	} else {
		try {
			auto res = load(semantic_id, ref, qp);
			return std::shared_ptr<const T>(res.release());
		} catch ( const DeliveryException &de ) {
			throw NoSuchElementException("Remote entry gone!");
		}
	}
}

template<class T>
std::unique_ptr<T> RemoteRetriever<T>::load(const std::string& semantic_id,
		const CacheRef& ref, QueryProfiler& qp) const {

	TypedNodeCacheKey key(LocalRetriever<T>::cache.get_type(), semantic_id,
			ref.entry_id);
	Log::debug("Fetching cache-entry from: %s:%d, key: %d", ref.host.c_str(),
			ref.port, ref.entry_id);

	auto dg = NodeServer::delivery_pool.get(ref.host,ref.port);

	try {
		auto resp = dg.get_connection().write_and_read(DeliveryConnection::CMD_GET_CACHED_ITEM,key);
		uint8_t rc = resp->read<uint8_t>();

		switch (rc) {
		case DeliveryConnection::RESP_OK: {
			FetchInfo mi(*resp);
			// Add the original costs
			qp.addTotalCosts(mi.profile);
			// Add the network-costs
			qp.addIOCost(mi.size);
			return read_item(*resp);
		}
		case DeliveryConnection::RESP_ERROR: {
			std::string err_msg = resp->read<std::string>();
			Log::debug("Remote-entry gone: %s", err_msg.c_str());
			throw DeliveryException(err_msg);
		}
		default: {
			Log::error("Delivery returned unknown code: %d", rc);
			throw DeliveryException("Delivery returned unknown code");
		}
		}
	} catch ( const NetworkException &ne ) {
		dg.set_faulty();
		throw DeliveryException("Connection failure");
	}
}

template<class T>
std::unique_ptr<T> RemoteRetriever<T>::read_item(
		BinaryReadBuffer& buffer) const {
	return make_unique<T>(buffer);
}

template<>
std::unique_ptr<GenericRaster> RemoteRetriever<GenericRaster>::read_item(
		BinaryReadBuffer& buffer) const {
	return GenericRaster::deserialize(buffer);
}

template<>
std::unique_ptr<GenericPlot> RemoteRetriever<GenericPlot>::read_item(
		BinaryReadBuffer& buffer) const {
	return GenericPlot::deserialize(buffer);
}

//
// INSTANTIATE ALL
//

template std::unique_ptr<GenericRaster> PuzzleUtil::process<GenericRaster>(GenericOperator&, const QueryRectangle&, const std::vector<Cube<3>>&, const std::vector<std::shared_ptr<const GenericRaster>>&, QueryProfiler&);
template std::unique_ptr<PointCollection> PuzzleUtil::process<PointCollection>(GenericOperator&, const QueryRectangle&, const std::vector<Cube<3>>&, const std::vector<std::shared_ptr<const PointCollection>>&, QueryProfiler&);
template std::unique_ptr<LineCollection> PuzzleUtil::process<LineCollection>(GenericOperator&, const QueryRectangle&, const std::vector<Cube<3>>&, const std::vector<std::shared_ptr<const LineCollection>>&, QueryProfiler&);
template std::unique_ptr<PolygonCollection> PuzzleUtil::process<PolygonCollection>(GenericOperator&, const QueryRectangle&, const std::vector<Cube<3>>&, const std::vector<std::shared_ptr<const PolygonCollection>>&, QueryProfiler&);
template std::unique_ptr<GenericPlot> PuzzleUtil::process<GenericPlot>(GenericOperator&, const QueryRectangle&, const std::vector<Cube<3>>&, const std::vector<std::shared_ptr<const GenericPlot>>&, QueryProfiler&);

template class LocalRetriever<GenericRaster> ;
template class LocalRetriever<GenericPlot> ;
template class LocalRetriever<PointCollection> ;
template class LocalRetriever<LineCollection> ;
template class LocalRetriever<PolygonCollection> ;

template class RemoteRetriever<GenericRaster> ;
template class RemoteRetriever<GenericPlot> ;
template class RemoteRetriever<PointCollection> ;
template class RemoteRetriever<LineCollection> ;
template class RemoteRetriever<PolygonCollection> ;

