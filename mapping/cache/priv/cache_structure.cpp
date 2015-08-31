/*
 * cache_structure.cpp
 *
 *  Created on: 09.08.2015
 *      Author: mika
 */

#include "cache/node/node_cache.h"
#include "cache/index/index_cache.h"

#include "cache/priv/cache_structure.h"
#include "cache/common.h"

#include "util/log.h"
#include "util/concat.h"

#include <iostream>

//
// Key
//

NodeCacheKey::NodeCacheKey(const std::string& semantic_id, uint64_t entry_id) :
	semantic_id(semantic_id), entry_id(entry_id) {
}

NodeCacheKey::NodeCacheKey(BinaryStream& stream) {
	stream.read(&semantic_id);
	stream.read(&entry_id);
}

void NodeCacheKey::toStream(BinaryStream& stream) const {
	stream.write(semantic_id);
	stream.write(entry_id);
}

std::string NodeCacheKey::to_string() const {
	return concat( "NodeCacheKey[ semantic_id: ", semantic_id, ", id: ", entry_id, "]");
}

//
// Bounds
//

CacheEntryBounds::CacheEntryBounds(SpatialReference sref, TemporalReference tref, QueryResolution::Type res_type,
	double x_res_from, double x_res_to, double y_res_from, double y_res_to ) :
  SpatioTemporalReference(sref,tref), res_type(res_type),
  x_res_from(x_res_from), x_res_to(x_res_to), y_res_from(y_res_from), y_res_to(y_res_to) {
}

CacheEntryBounds::CacheEntryBounds(const SpatioTemporalResult& result) :
	SpatioTemporalReference(result.stref), res_type(QueryResolution::Type::NONE),
	x_res_from(0), x_res_to(0), y_res_from(0), y_res_to(0) {
}

CacheEntryBounds::CacheEntryBounds(const GridSpatioTemporalResult& result) :
	SpatioTemporalReference(result.stref), res_type(QueryResolution::Type::PIXELS) {
	double ohspan = x2 - x1;
	double ovspan = y2 - y1;

	// Enlarge result by degrees of 1/100 a pixel in each direction
	double h_spacing = ohspan / result.width / 100.0;
	double v_spacing = ovspan / result.height / 100.0;

	x1 = x1 - h_spacing;
	x2 = x2 + h_spacing;
	y1 = y1 - v_spacing;
	y2 = y2 + v_spacing;

	// Calc resolution bounds: (res*.75, res*1.5]
	double h_pixel_per_deg = result.width / ohspan;
	double v_pixel_per_deg = result.height / ovspan;

	x_res_from = h_pixel_per_deg * 0.75;
	x_res_to = h_pixel_per_deg * 1.5;

	y_res_from = v_pixel_per_deg * 0.75;
	y_res_to = v_pixel_per_deg * 1.5;
}

CacheEntryBounds::CacheEntryBounds(BinaryStream& stream) :
	SpatioTemporalReference(stream) {
	stream.read(&res_type);
	stream.read(&x_res_from);
	stream.read(&x_res_to);
	stream.read(&y_res_from);
	stream.read(&y_res_to);
}

void CacheEntryBounds::toStream(BinaryStream& stream) const {
	SpatialReference::toStream(stream);
	TemporalReference::toStream(stream);
	stream.write(res_type);
	stream.write(x_res_from);
	stream.write(x_res_to);
	stream.write(y_res_from);
	stream.write(y_res_to);
}

bool CacheEntryBounds::matches(const QueryRectangle& query) const {
	if ( SpatialReference::contains(query) &&
		 TemporalReference::contains(query) ) {

		if ( res_type == QueryResolution::Type::NONE )
			return true;
		else if ( res_type == QueryResolution::Type::PIXELS ) {

			double q_x_res = (double) query.xres / (query.x2 - query.x1);
			double q_y_res = (double) query.yres / (query.y2 - query.y1);

			return x_res_from <  q_x_res &&
				   x_res_to   >= q_x_res &&
				   y_res_from <  q_y_res &&
				   y_res_to   >= q_y_res;
		}
		else
			throw ArgumentException("Unknown QueryResolution::Type in QueryRectangle");
	}
	return false;
}

double CacheEntryBounds::get_coverage(const QueryRectangle& query) const {
	if ( x1 > query.x2 || x2 < query.x1 ||
		 y1 > query.y2 || y2 < query.y1 )
			return 0.0;

	double ix1 = std::max(x1,query.x1),
		   ix2 = std::min(x2,query.x2),
		   iy1 = std::max(y1,query.y1),
		   iy2 = std::min(y2,query.y2);

	double iarea    = std::abs((ix2-ix1) * (iy2-iy1));
	double qarea    = std::abs((query.x2-query.x1) * (query.y2-query.y1));
	double coverage = iarea / qarea;

	if ( res_type == QueryResolution::Type::NONE )
		return coverage;
	else if ( res_type == QueryResolution::Type::PIXELS ) {
		double q_x_res = (double) query.xres / (query.x2 - query.x1);
		double q_y_res = (double) query.yres / (query.y2 - query.y1);

		if ( x_res_from <  q_x_res &&
			 x_res_to   >= q_x_res &&
			 y_res_from <  q_y_res &&
			 y_res_to   >= q_y_res )
			return coverage;
		else
			return 0.0;
	}
	throw ArgumentException("Unknown QueryResolution::Type in QueryRectangle");
}

std::string CacheEntryBounds::to_string() const {
	return concat("CacheEntryBounds[",
		   "x1: ", x1,
		   ", x2: ", x2,
		   ", y1: ", y1,
		   ", y2: ", y2,
		   ", t1: ", t1,
		   ", t2: ", t2,
		   ", res_type: ", (int) res_type,
		   ", x_res_from: ", x_res_from,
		   ", x_res_to: ", x_res_to,
		   ", y_res_from: ", y_res_from,
		   ", y_res_to: ", y_res_to, "]");
}

//
// CacheEntry
//
CacheEntry::CacheEntry(CacheEntryBounds bounds, uint64_t size) :
	bounds(bounds), size(size), last_access(time(nullptr)), access_count(1){
}

CacheEntry::CacheEntry(BinaryStream& stream) : bounds(stream) {
	stream.read(&size);
	stream.read(&last_access);
	stream.read(&access_count);
}

void CacheEntry::toStream(BinaryStream& stream) const {
	bounds.toStream(stream);
	stream.write(size);
	stream.write(last_access);
	stream.write(access_count);
}

std::string CacheEntry::to_string() const {
	return concat("CacheEntry[size: ", size, ", last_access: ", last_access, ", access_count: ", access_count, ", bounds: ", bounds.to_string(), "]");
}

//
// Query Info
//

template<typename KType>
CacheQueryInfo<KType>::CacheQueryInfo(double coverage, double x1, double x2, double y1, double y2, KType key) :
	coverage(coverage), x1(x1), x2(x2), y1(y1), y2(y2), key(key) {
}

template<typename KType>
bool CacheQueryInfo<KType>::operator <(const CacheQueryInfo& b) const {
	return get_score() < b.get_score();
}

template<typename KType>
std::string CacheQueryInfo<KType>::to_string() const {
	return concat("CacheQueryInfo: [", x1, ",", x2, "]x[", y1, ",", y2, "], coverage: ", coverage);
}

template<typename KType>
double CacheQueryInfo<KType>::get_score() const {
	// Scoring over coverage and area
	return coverage / ((x2-x1)*(y2-y1));
}

//
// Query Result
//
template<typename KType>
CacheQueryResult<KType>::CacheQueryResult(const QueryRectangle& query) :
	covered( CacheCommon::empty_geom() ),
	remainder( CacheCommon::create_square(query.x1,query.y1,query.x2,query.y2) ),
	coverage(0) {
}

template<typename KType>
CacheQueryResult<KType>::CacheQueryResult( GeomP& covered, GeomP& remainder, double coverage, std::vector<KType> keys) :
	covered( std::move(covered) ),
	remainder( std::move(remainder) ),
	coverage(coverage),
	keys(keys) {
	if ( !this->remainder->isEmpty() && !this->remainder->isRectangle() )
		throw ArgumentException("Remainder must be a rectangle");
}

template<typename KType>
CacheQueryResult<KType>::CacheQueryResult(const CacheQueryResult& r) :
	covered( GeomP( r.covered->clone() ) ),
	remainder( GeomP( r.remainder->clone() ) ),
	coverage(r.coverage),
	keys(r.keys) {
}

template<typename KType>
CacheQueryResult<KType>::CacheQueryResult(CacheQueryResult&& r) :
	covered( std::move(r.covered) ),
	remainder( std::move(r.remainder) ),
	coverage(r.coverage),
	keys(std::move(keys)) {
}

template<typename KType>
CacheQueryResult<KType>& CacheQueryResult<KType>::operator =(const CacheQueryResult& r) {
	covered.reset( r.covered->clone() );
	remainder.reset( r.remainder->clone() );
	coverage = r.coverage;
	keys = r.keys;
	return *this;
}

template<typename KType>
CacheQueryResult<KType>& CacheQueryResult<KType>::operator =(CacheQueryResult&& r) {
	covered = std::move(r.covered);
	remainder = std::move(r.remainder);
	coverage = r.coverage;
	keys = std::move(r.keys);
	return *this;
}

template<typename KType>
bool CacheQueryResult<KType>::has_remainder() const {
	return !remainder->isEmpty();
}

template<typename KType>
bool CacheQueryResult<KType>::has_hit() const {
	return !covered->isEmpty();
}

template<typename KType>
std::string CacheQueryResult<KType>::to_string() const {
	return concat( "CacheQueryResult[",
	"has_hit: ", has_hit(),
	",  has_remainder: ", has_remainder(),
	",  coverage: ", coverage,
	",  covered: ", covered->toString(),
	",  remainder: ", remainder->toString(),
	",  num keys: ", keys.size(),
	"]");
}


//
// CacheRef
//

NodeCacheRef::NodeCacheRef(const NodeCacheKey& key, const CacheEntry& entry) :
	NodeCacheKey(key), CacheEntry(entry) {
}

NodeCacheRef::NodeCacheRef(const std::string semantic_id, uint64_t entry_id, const CacheEntry& entry) :
	NodeCacheKey(semantic_id,entry_id), CacheEntry(entry) {
}

NodeCacheRef::NodeCacheRef(BinaryStream& stream) :
	NodeCacheKey(stream), CacheEntry(stream) {
}

void NodeCacheRef::toStream(BinaryStream& stream) const {
	NodeCacheKey::toStream(stream);
	CacheEntry::toStream(stream);
}

std::string NodeCacheRef::to_string() const {
	return concat("CacheRef[ key: ", NodeCacheKey::to_string(), ", entry: ", CacheEntry::to_string(), "]");
}

//////////////////////////////////////////////////////////////
//
// Structure
//
//////////////////////////////////////////////////////////////


template<typename KType, typename EType>
void CacheStructure<KType, EType>::put(const KType& key, const std::shared_ptr<EType>& result) {
	std::lock_guard<std::mutex> guard(mtx);
//	Log::trace("Inserting new entry. Id: %d", key );
	entries.emplace(key, result);

}

template<typename KType, typename EType>
std::shared_ptr<EType> CacheStructure<KType, EType>::get(const KType& key) const {
	std::lock_guard<std::mutex> guard(mtx);
	try {
		return entries.at(key);
	} catch (const std::out_of_range &oor) {
		throw NoSuchElementException("No cache-entry found");
	}
}

template<typename KType, typename EType>
std::shared_ptr<EType> CacheStructure<KType, EType>::remove(const KType& key) {
	std::lock_guard<std::mutex> guard(mtx);
	auto iter = entries.find(key);
	if ( iter != entries.end() ) {
		auto result = iter->second;
		entries.erase(iter);
		return result;
	}
	throw NoSuchElementException("No cache-entry found");
}

template<typename KType, typename EType>
const CacheQueryResult<KType> CacheStructure<KType, EType>::query(const QueryRectangle& spec) const {
	std::lock_guard<std::mutex> guard(mtx);

	Log::trace("Querying cache for: %s", CacheCommon::qr_to_string(spec).c_str() );

	// Get intersecting entries
	auto partials = get_query_candidates( spec );

	// No candidates found
	if ( partials.empty() ) {
		Log::trace("No candidates cached.");
		return CacheQueryResult<KType>(spec);
	}

	std::vector<KType> ids;
	std::unique_ptr<geos::geom::Geometry> remainder;
	std::unique_ptr<geos::geom::Geometry> p_union = CacheCommon::empty_geom();
	std::unique_ptr<geos::geom::Geometry> qbox    = CacheCommon::create_square(spec.x1,spec.y1,spec.x2,spec.y2);

	// Add entries until we cover the whole area or nothing is left
	while (!partials.empty() && !p_union->contains( qbox.get() )) {
		auto qi = partials.top();
		Log::trace("Candidate: %s", qi.to_string().c_str());

		// Create the intersection and check if it enhances the covered area
		std::unique_ptr<geos::geom::Polygon> box = CacheCommon::create_square(
			std::max(spec.x1,qi.x1),
			std::max(spec.y1,qi.y1),
			std::min(spec.x2,qi.x2),
			std::min(spec.y2,qi.y2)
		);
		if ( !p_union->contains( box.get() ) ) {
			p_union = CacheCommon::union_geom( p_union, box );
			Log::trace("Added candidate. Covered area is now: %s", p_union->toString().c_str());
			ids.push_back(qi.key);
		}
		else {
			Log::trace("Omitting candidate, does not enlarge covered area");
		}
		partials.pop();
	}

	double coverage;
	// Full coverage
	if ( p_union->contains( qbox.get() ) ) {
		Log::trace("Query can be fully answered from cache.");
		remainder = CacheCommon::empty_geom();
		coverage = 1;
	}
	// Calculate remainder
	else {
		geos::geom::Geometry *isect = qbox->intersection( p_union.get() );
		geos::geom::Geometry *rem_poly = qbox->difference(isect);
		coverage = isect->getArea() / qbox->getArea();
		remainder.reset(rem_poly->getEnvelope());
		delete rem_poly;
		delete isect;
		Log::trace("Query can be partially answered from cache. Remainder rectangle: %s", remainder->toString().c_str());
	}
	return CacheQueryResult<KType>( p_union, remainder, coverage, ids);


}

template<typename KType, typename EType>
std::priority_queue<CacheQueryInfo<KType>> CacheStructure<KType, EType>::get_query_candidates(
	const QueryRectangle& spec) const {

	Log::trace("Fetching candidates for query: %s", CacheCommon::qr_to_string(spec).c_str() );
	std::priority_queue<CacheQueryInfo<KType>> partials;
	for (auto &e : entries) {
		auto &bounds = e.second->bounds;
		double coverage = bounds.get_coverage(spec);
		Log::trace("Coverage for entry %d: %f", e.first, coverage);
		if ( coverage > 0 )
			partials.push(CacheQueryInfo<KType>( coverage, bounds.x1, bounds.x2, bounds.y1, bounds.y2, e.first ));
	}
	Log::trace("Found %d candidates for query: %s", partials.size(), CacheCommon::qr_to_string(spec).c_str() );
	return partials;
}

template<typename KType, typename EType>
std::vector<std::shared_ptr<EType> > CacheStructure<KType, EType>::get_all() const {
	std::vector<std::shared_ptr<EType> > result;
	for (auto &e : entries) {
		result.push_back(e.second);
	}
	return result;
}

template class CacheQueryResult<uint64_t>;
template class CacheStructure<uint64_t, NodeCacheEntry<GenericRaster>>;

template class CacheQueryResult<std::pair<uint32_t,uint64_t>>;
template class CacheStructure<std::pair<uint32_t, uint64_t>, IndexCacheEntry> ;
