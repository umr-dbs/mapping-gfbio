/*
 * cache_structure.cpp
 *
 *  Created on: 09.08.2015
 *      Author: mika
 */


#include "cache/priv/cache_structure.h"
#include "cache/node/node_cache.h"
#include "cache/index/index_cache.h"
#include "cache/common.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include "util/log.h"
#include "util/concat.h"

#include <iostream>
#include <chrono>
#include <limits>

///////////////////////////////////////////////////////////
//
// CACHE QUERY INFO
//
///////////////////////////////////////////////////////////

template<typename KType>
CacheQueryInfo<KType>::CacheQueryInfo(const KType &key, const std::shared_ptr<const CacheEntry> &entry, double score) :
	key(key), entry(entry), score(score) {
}

template<typename KType>
bool CacheQueryInfo<KType>::operator <(const CacheQueryInfo& b) const {
	return score < b.score;
}

template<typename KType>
std::string CacheQueryInfo<KType>::to_string() const {
	return concat("CacheQueryInfo: ", entry->to_string(), ", score: ", score);
}

///////////////////////////////////////////////////////////
//
// CACHE QUERY RESULT
//
///////////////////////////////////////////////////////////

template<typename KType>
CacheQueryResult<KType>::CacheQueryResult(const QueryRectangle& query) :
	covered(query) {
	remainder.push_back( Cube3(query.x1,query.x2,query.y1,query.y2,query.t1,query.t2) );
}

template<typename KType>
CacheQueryResult<KType>::CacheQueryResult( QueryRectangle &&query, std::vector<Cube<3>> &&remainder, std::vector<KType> &&keys) :
	covered(query),
	keys(keys),
	remainder( remainder ) {
}

template<typename KType>
bool CacheQueryResult<KType>::has_remainder() const {
	return !remainder.empty();
}

template<typename KType>
bool CacheQueryResult<KType>::has_hit() const {
	return !keys.empty();
}

template<typename KType>
std::string CacheQueryResult<KType>::to_string() const {
	return concat( "CacheQueryResult[",
	"has_hit: ", has_hit(),
	",  has_remainder: ", has_remainder(),
	",  num remainders: ", remainder.size(),
	",  num keys: ", keys.size(),
	"]");
}


//////////////////////////////////////////////////////////////
//
// CACHE STRUCTURE
//
//////////////////////////////////////////////////////////////

template<typename KType, typename EType>
CacheStructure<KType, EType>::CacheStructure(const std::string &semantic_id) : semantic_id(semantic_id), _size(0) {
}


template<typename KType, typename EType>
void CacheStructure<KType, EType>::put(const KType& key, const std::shared_ptr<EType>& result) {
	ExclusiveLockGuard g(lock);
//	Log::trace("Inserting new entry. Id: %d", key );
	_size += result->size;
	entries.emplace(key, result);
}

template<typename KType, typename EType>
std::shared_ptr<EType> CacheStructure<KType, EType>::get(const KType& key) const {
	SharedLockGuard g(lock);
	try {
		return entries.at(key);
	} catch (const std::out_of_range &oor) {
		throw NoSuchElementException("No cache-entry found");
	}
}

template<typename KType, typename EType>
std::shared_ptr<EType> CacheStructure<KType, EType>::remove(const KType& key) {
	ExclusiveLockGuard g(lock);
	auto iter = entries.find(key);
	if ( iter != entries.end() ) {
		auto result = iter->second;
		entries.erase(iter);
		_size -= result->size;
		return result;
	}
	throw NoSuchElementException("No cache-entry found");
}

template<typename KType, typename EType>
std::vector<std::shared_ptr<EType> > CacheStructure<KType, EType>::get_all() const {
	SharedLockGuard g(lock);
	std::vector<std::shared_ptr<EType> > result;
	result.reserve(entries.size());
	for (auto &e : entries) {
		result.push_back(e.second);
	}
	return result;
}

//
// QUERY STUFF
//

template<typename KType, typename EType>
const CacheQueryResult<KType> CacheStructure<KType, EType>::query(const QueryRectangle& spec) const {
	SharedLockGuard g(lock);

	Log::trace("Querying cache for: %s", CacheCommon::qr_to_string(spec).c_str() );

	const QueryCube qc(spec);

	// Get intersecting entries
	auto candidates = get_query_candidates( qc );

	// No candidates found
	if ( candidates.empty() ) {
		Log::trace("No candidates cached.");
		return CacheQueryResult<KType>(spec);
	}

	std::vector<CacheQueryInfo<KType>> used_entries;
	std::vector<KType> ids;
	std::vector<Cube<3>> remainders{qc}, tmp_remainders;

	ids.reserve(candidates.size());
	used_entries.reserve(candidates.size());

	while ( !candidates.empty() && !remainders.empty() ) {
		bool used = false;
		const CacheQueryInfo<KType> &info = candidates.top();

		// Skip incompatible resolutions
		if ( spec.restype == QueryResolution::Type::PIXELS &&
			 !used_entries.empty() &&
			 !CacheCommon::resolution_matches(
				 info.entry->bounds, used_entries.front().entry->bounds)	) {
			candidates.pop();
			continue;
		}


		// Dissect remainders
		tmp_remainders.clear();

		for ( auto &r : remainders ) {
			if ( info.entry->bounds.intersects(r) ) {
				used = true;
				auto split = r.dissect_by(info.entry->bounds);
				// Insert new remainders
				if ( !split.empty() )
					tmp_remainders.insert(tmp_remainders.end(),split.begin(),split.end());
			}
			else
				tmp_remainders.push_back(r);
		}
		std::swap(remainders,tmp_remainders);

		if ( used ) {
			ids.push_back(info.key);
			used_entries.push_back( std::move(info) );
		}
		candidates.pop();
	}

	// Union remainders
	auto u_rems = union_remainders(remainders);

	double rem_volume = 0;
	for ( auto &rem : u_rems ) {
		rem_volume += rem.volume();
	}
	// Return miss if we have a low coverage (<10%)
	if ( rem_volume/ qc.volume() > 0.9 )
		return CacheQueryResult<KType>( spec );


	// Entend expected result
	auto new_query = enlarge_expected_result(qc, used_entries, u_rems);

	// Stretch timespan of raster-data
	if ( spec.restype == QueryResolution::Type::PIXELS ) {
		for ( auto &rem : u_rems )
			rem.set_dimension(2, new_query.t1, new_query.t2);
	}

	return CacheQueryResult<KType>( std::move(new_query), std::move(u_rems), std::move(ids) );
}

template<typename KType, typename EType>
std::priority_queue<CacheQueryInfo<KType>> CacheStructure<KType, EType>::get_query_candidates(
	const QueryCube& qc) const {

//	Log::trace("Fetching candidates for query: %s", CacheCommon::qr_to_string(spec).c_str() );
	std::priority_queue<CacheQueryInfo<KType>> partials;

	for (auto &e : entries) {
		CacheCube &bounds = e.second->bounds;

		if ( qc.epsg == bounds.epsg &&
			 qc.timetype == bounds.timetype &&
			 bounds.resolution_info.matches(qc) &&
			 bounds.intersects(qc) ) {

			// Raster
			if ( qc.restype == QueryResolution::Type::PIXELS &&
				!bounds.get_timespan().contains( qc.get_dimension(2) ) )
				continue;

			// Coverage = score for now
			double score = bounds.intersect(qc).volume() / qc.volume();
			Log::trace("Score for entry %s: %f", key_to_string(e.first).c_str(), score);
			partials.push( CacheQueryInfo<KType>( e.first, e.second, score ) );

			// Short circuit full hits
			if ( (1.0-score) <= std::numeric_limits<double>::epsilon() ) {
				break;
			}

		}
	}
//	Log::trace("Found %d candidates for query: %s", partials.size(), CacheCommon::qr_to_string(spec).c_str() );
	return std::move(partials);
}

template<typename KType, typename EType>
std::vector<Cube<3> > CacheStructure<KType, EType>::union_remainders(
		std::vector<Cube<3> >& work) const {

	std::vector<Cube<3>> result;

	while ( !work.empty() ) {
		// Take one cube
		auto current = work.back();
		work.pop_back();

		// See if it can be combined with any of the remaining cubes
		// If so: start over since we maybe can add more now
		auto i = work.begin();
		while ( i != work.end() ) {
			auto tmp = current.combine(*i);
			if ( tmp.volume() < (current.volume() + i->volume()) * 1.01 ) {
				current = std::move(tmp);
				work.erase(i);
				i = work.begin();
			}
			else
				i++;
		}
		result.push_back(current);
	}
	return result;
}

template<typename KType, typename EType>
QueryRectangle CacheStructure<KType, EType>::enlarge_expected_result( const QueryCube &qc,
	const std::vector<CacheQueryInfo<KType>> &hits, const std::vector<Cube<3>> &remainders) const {

	// Calculated maximum covered cube
	double values[6] = { -std::numeric_limits<double>::infinity(),
						  std::numeric_limits<double>::infinity(),
						 -std::numeric_limits<double>::infinity(),
						  std::numeric_limits<double>::infinity(),
						 -std::numeric_limits<double>::infinity(),
						  std::numeric_limits<double>::infinity() };


	// If we have a raster, we extend it in spatial dimensions and
	// enlarge time-interval to t1 = max(hits.t1) and t2 = min(hits.t2)
	// since there MUST NOT be two results with different time-intervals
	int check_dims = (qc.restype == QueryResolution::Type::PIXELS) ? 2 : 3;

	// Calculate coverage and check which edges may be extended
	// Only extend edges untouched by a remainder
	for ( auto &rem : remainders ) {
		for ( int i = 0; i < check_dims; i++ ) {
			auto &rdim = rem.get_dimension(i);
			auto &qdim = qc.get_dimension(i);
			// Limit to original query if a remainder touches query-bounds
			if ( rdim.a <= qdim.a )
				values[2*i] = qdim.a;
			if ( rdim.b >= qdim.b )
				values[2*i+1] = qdim.b;
		}
	}

	// Do extend
	for ( auto &cqi : hits ) {
		for ( int i = 0; i < 3; i++ ) {
			auto &cdim = cqi.entry->bounds.get_dimension(i);
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


	QueryResolution qr = QueryResolution::none();

	// RASTER ONLY
	// calculate resolution
	if ( qc.restype == QueryResolution::Type::PIXELS ) {
		int w = std::ceil( qc.pixel_scale_x / (values[1]-values[0]));
		int h = std::ceil( qc.pixel_scale_y / (values[3]-values[2]));
		qr = QueryResolution::pixels(w,h);
	}

	return QueryRectangle(
		SpatialReference( qc.epsg, values[0], values[2], values[1], values[3] ),
		TemporalReference( qc.timetype, values[4], values[5] ),
		qr
	);
}


//
// END QUERY STUFF
//


template<typename KType, typename EType>
uint64_t CacheStructure<KType, EType>::size() const {
	return _size;
}

template<typename KType, typename EType>
uint64_t CacheStructure<KType, EType>::num_elements() const {
	return entries.size();
}

template<typename KType, typename EType>
std::string CacheStructure<KType, EType>::key_to_string(const KType &key) const {
	return std::to_string(key);
}

template<>
std::string CacheStructure< std::pair<uint32_t, uint64_t>, IndexCacheEntry>::key_to_string(const std::pair<uint32_t, uint64_t> &key) const {
	return concat("(",key.first,":",key.second,")");
}


//////////////////////////////////////////////////////////////
//
// Cache
//
//////////////////////////////////////////////////////////////

template<typename KType, typename EType>
const CacheQueryResult<KType> Cache<KType, EType>::query(
	const std::string& semantic_id, const QueryRectangle& qr) const {
	try {
		return get_cache(semantic_id).query(qr);
	} catch ( const NoSuchElementException &nse ) {
		return CacheQueryResult<KType>(qr);
	}
}

template<typename KType, typename EType>
void Cache<KType, EType>::put_int(const std::string& semantic_id,
		const KType& key, const std::shared_ptr<EType>& entry) {
	get_cache(semantic_id,true).put( key, entry );
}

template<typename KType, typename EType>
std::shared_ptr<EType> Cache<KType, EType>::get_int(
	const std::string& semantic_id, const KType& key) const {
	return get_cache(semantic_id).get(key);
}

template<typename KType, typename EType>
std::shared_ptr<EType> Cache<KType, EType>::remove_int(
	const std::string& semantic_id,const KType& key) {
	return get_cache(semantic_id).remove(key);
	// TODO: Find a safe way to remove structures
//	auto &cache = get_cache(semantic_id);
//	auto res = cache.remove(key);
//	if ( cache.num_elements() == 0 )
//		caches.erase(semantic_id);
//	return res;
}

template<typename KType, typename EType>
std::unordered_map<std::string, std::vector<std::shared_ptr<EType>> > Cache<KType,
		EType>::get_all_int() const {
	std::lock_guard<std::mutex> guard(mtx);

	std::unordered_map<std::string, std::vector<std::shared_ptr<EType>>> result;
	for ( auto &p : caches ) {
		result.emplace( p.first, p.second->get_all() );
	}
	return result;
}

template<typename KType, typename EType>
CacheStructure<KType, EType>& Cache<KType, EType>::get_cache(
		const std::string& semantic_id, bool create) const {

	std::lock_guard<std::mutex> guard(mtx);
	Log::trace("Retrieving cache-structure for semantic_id: %s", semantic_id.c_str() );
	auto got = caches.find(semantic_id);
	if (got == caches.end() && create) {
		Log::trace("No cache-structure found for semantic_id: %s. Creating.", semantic_id.c_str() );
		auto e = caches.emplace(semantic_id, make_unique<CacheStructure<KType,EType>>(semantic_id));
		return *e.first->second;
	}
	else if (got != caches.end())
		return *got->second;
	else
		throw NoSuchElementException("No structure present for given semantic id");
}

template class CacheQueryResult<uint64_t>;
template class CacheStructure<uint64_t, NodeCacheEntry<GenericRaster>>;
template class CacheStructure<uint64_t, NodeCacheEntry<PointCollection>>;
template class CacheStructure<uint64_t, NodeCacheEntry<LineCollection>>;
template class CacheStructure<uint64_t, NodeCacheEntry<PolygonCollection>>;
template class CacheStructure<uint64_t, NodeCacheEntry<GenericPlot>>;

template class Cache<uint64_t, NodeCacheEntry<GenericRaster>>;
template class Cache<uint64_t, NodeCacheEntry<PointCollection>>;
template class Cache<uint64_t, NodeCacheEntry<LineCollection>>;
template class Cache<uint64_t, NodeCacheEntry<PolygonCollection>>;
template class Cache<uint64_t, NodeCacheEntry<GenericPlot>>;


template class CacheQueryResult<std::pair<uint32_t,uint64_t>>;
template class CacheStructure<std::pair<uint32_t, uint64_t>, IndexCacheEntry> ;
template class Cache<std::pair<uint32_t, uint64_t>, IndexCacheEntry> ;
