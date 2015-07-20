/*
 * cache.cpp
 *
 *  Created on: 12.05.2015
 *      Author: mika
 */

#include "cache/cache.h"
#include "cache/common.h"
#include "cache/priv/transfer.h"
#include "util/exceptions.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <iostream>
#include <vector>
#include <queue>
#include <sstream>
#include <unordered_map>

////////////////////////////////////////////////////////
//
// Query-Result
//
////////////////////////////////////////////////////////


STQueryResult::STQueryResult(const QueryRectangle& query) :
	covered( CacheCommon::empty_geom() ),
	remainder( CacheCommon::create_square(query.x1,query.y1,query.x2,query.y2) ),
	coverage(0) {
}

STQueryResult::STQueryResult( GeomP& covered, GeomP& remainder, double coverage, const std::vector<uint64_t>& ids) :
	covered( std::move(covered) ),
	remainder( std::move(remainder) ),
	coverage(coverage),
	ids( ids ) {
	if ( !this->remainder->isEmpty() && !this->remainder->isRectangle() )
		throw ArgumentException("Remainder must be a rectangle");
}

STQueryResult::STQueryResult(const STQueryResult& r) :
	covered( GeomP( r.covered->clone() ) ),
	remainder( GeomP( r.remainder->clone() ) ),
	coverage(r.coverage),
	ids( r.ids ) {
}

STQueryResult::STQueryResult(STQueryResult&& r) :
	covered( std::move(r.covered) ),
	remainder( std::move(r.remainder) ),
	coverage(r.coverage),
	ids( std::move(r.ids) ) {
}


STQueryResult& STQueryResult::operator =(const STQueryResult& r) {
	covered.reset( r.covered->clone() );
	remainder.reset( r.remainder->clone() );
	coverage = r.coverage;
	ids = r.ids;
	return *this;
}

STQueryResult& STQueryResult::operator =(STQueryResult&& r) {
	covered = std::move(r.covered);
	remainder = std::move(r.remainder);
	coverage = r.coverage;
	ids = std::move(r.ids);
	return *this;
}

bool STQueryResult::has_remainder() {
	return !remainder->isEmpty();
}

bool STQueryResult::has_hit() {
	return !covered->isEmpty();
}

std::string STQueryResult::to_string() {
	std::ostringstream ss;
	ss << "STQueryResult:" << std::endl;
	ss << "  has_hit: " << has_hit() << std::endl;
	ss << "  has_remainder: " << has_remainder() << std::endl;
	ss << "  coverage: " << coverage << std::endl;
	ss << "  covered: " << covered->toString() << std::endl;
	ss << "  remainder: " << remainder->toString() << std::endl;
	ss << "  ids: [";
	for ( std::vector<uint64_t>::size_type i = 0; i < ids.size(); i++ ) {
		if ( i > 0 )
			ss << ", ";
		ss << ids.at(i);
	}
	ss << "]";
	return ss.str();
}

///////////////////////////////////////////////////////////////////
//
// CACHE-STRUCTURE
//
///////////////////////////////////////////////////////////////////

template<typename EType>
STCacheStructure<EType>::~STCacheStructure() {
}

template<typename EType>
const STQueryResult STCacheStructure<EType>::query(const QueryRectangle& spec) const {

	// Get intersecting entries
	auto partials = get_query_candidates( spec );

	Log::trace("Querying cache for: %s", CacheCommon::qr_to_string(spec).c_str() );

	// No candidates found
	if ( partials->empty() ) {
		Log::trace("No candidates cached.");
		return STQueryResult(spec);
	}

	std::vector<uint64_t> ids;
	STQueryResult::GeomP remainder;
	STQueryResult::GeomP p_union = CacheCommon::empty_geom();
	STQueryResult::GeomP qbox    = CacheCommon::create_square(spec.x1,spec.y1,spec.x2,spec.y2);

	// Add entries until we cover the whole area or nothing is left
	while (!partials->empty() && !p_union->contains( qbox.get() )) {
		auto qi = partials->top();
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
			ids.push_back(qi.cache_id);
		}
		else {
			Log::trace("Omitting candidate, does not enlarge covered area");
		}
		partials->pop();
	}

	double coverage;
	// Full coverage
	if ( p_union->contains( qbox.get() ) ) {
		Log::trace("Query can be fully answered from cache.");
		remainder = CacheCommon::empty_geom();
		coverage = 0;
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
	return STQueryResult( p_union, remainder, coverage, ids);
}

///////////////////////////////////////////////////////////////////
//
// CACHE-ENTRY
//
///////////////////////////////////////////////////////////////////

template<typename EType>
class STCacheEntry {
public:
	STCacheEntry(uint64_t id, const std::shared_ptr<EType> &result, std::unique_ptr<STEntryBounds> &bounds, uint64_t size);
	const uint64_t id;
	const std::shared_ptr<EType> result;
	const std::unique_ptr<STEntryBounds> bounds;
	const uint64_t size;
};

template<typename EType>
STCacheEntry<EType>::STCacheEntry(uint64_t id, const std::shared_ptr<EType> &result,
		std::unique_ptr<STEntryBounds> &bounds, uint64_t size)  :
	id(id), result(result), bounds(std::move(bounds)), size(sizeof(STCacheEntry) + size) {
}

///////////////////////////////////////////////////////////////////
//
// MAP-CACHE-STRUCTURE
//
///////////////////////////////////////////////////////////////////

template<typename EType>
class STMapCacheStructure: public STCacheStructure<EType> {
public:
	STMapCacheStructure();
	virtual ~STMapCacheStructure();
	virtual uint64_t insert(const std::unique_ptr<EType> &result);
	virtual std::unique_ptr<EType> get_copy(const uint64_t id) const;
	virtual const std::shared_ptr<EType> get(const uint64_t id) const;
	virtual uint64_t get_entry_size(const uint64_t id) const;
	virtual void remove(const uint64_t id);
protected:
	virtual std::unique_ptr<std::priority_queue<STQueryInfo>> get_query_candidates( const QueryRectangle &spec ) const;
	virtual std::unique_ptr<EType> copy(const EType &content) const = 0;
	virtual std::unique_ptr<STEntryBounds> create_bounds(const EType &content) const = 0;
	virtual uint64_t get_content_size(const EType &content) const = 0;
private:
	std::unordered_map<uint64_t, std::shared_ptr<STCacheEntry<EType>>> entries;
	uint64_t next_id;
};

template<typename EType>
STMapCacheStructure<EType>::STMapCacheStructure() : next_id(1) {
}

template<typename EType>
STMapCacheStructure<EType>::~STMapCacheStructure() {
	//Nothing to-do
}

template<typename EType>
uint64_t STMapCacheStructure<EType>::insert(const std::unique_ptr<EType> &result) {
	auto cpy = copy(*result);
	std::shared_ptr<EType> sp(cpy.release());

	uint64_t id   = next_id++;
	uint64_t size = get_content_size(*result) + sizeof(STCacheEntry<EType>);
	std::unique_ptr<STEntryBounds> bounds = create_bounds(*result);

	Log::trace("Inserting new entry. Id: %d, size: %d, bounds: %s", id, size, bounds->to_string().c_str() );

	std::shared_ptr<STCacheEntry<EType>> entry(
			new STCacheEntry<EType>( id,
									 sp,
									 bounds,
									 get_content_size(*result) ) );

	entries.emplace(id, entry);
	return id;
}

template<typename EType>
const std::shared_ptr<EType> STMapCacheStructure<EType>::get(const uint64_t id) const {
	Log::trace("Retrieving cache-entry with id: %d", id);
	try {
		auto &e = entries.at(id);
		return e->result;
	} catch (std::out_of_range &oor) {
		Log::trace("No entry found for id: %d", id);
		std::ostringstream msg;
		msg << "No cache-entry with id: " << id;
		throw NoSuchElementException(msg.str());
	}
}

template<typename EType>
std::unique_ptr<EType> STMapCacheStructure<EType>::get_copy(const uint64_t id) const {
	Log::trace("Returning copy of entry with id: %d", id);
	return copy(*get(id));
}

template<typename EType>
uint64_t STMapCacheStructure<EType>::get_entry_size(const uint64_t id) const {
	try {
		auto &e = entries.at(id);
		return e->size;
	} catch (std::out_of_range &oor) {
		std::ostringstream msg;
		msg << "No cache-entry with id: " << id;
		throw NoSuchElementException(msg.str());
	}
}

template<typename EType>
void STMapCacheStructure<EType>::remove(const uint64_t id) {
	Log::trace("Removing entry with id: %d", id);
	entries.erase(id);
}

template<typename EType>
std::unique_ptr<std::priority_queue<STQueryInfo> > STMapCacheStructure<EType>::get_query_candidates(
		const QueryRectangle& spec) const {
	Log::trace("Fetching candidates for query: %s", CacheCommon::qr_to_string(spec).c_str() );
	std::unique_ptr<std::priority_queue<STQueryInfo>> partials = make_unique<std::priority_queue<STQueryInfo>>();
	for (auto &e : entries) {
		STEntryBounds &bounds = *e.second->bounds;
		double coverage = bounds.get_coverage(spec);
		Log::trace("Coverage for entry %d: %f", e.first, coverage);
		if ( coverage > 0 )
			partials->push(STQueryInfo( coverage, bounds.x1, bounds.x2, bounds.y1, bounds.y2, e.first ));
	}
	Log::trace("Found %d candidates for query: %s", partials->size(), CacheCommon::qr_to_string(spec).c_str() );
	return partials;
}



///////////////////////////////////////////////////////////////////
//
// RASTER-MAP-CACHE-STRUCTURE
//
///////////////////////////////////////////////////////////////////

class STRasterCacheStructure: public STMapCacheStructure<GenericRaster> {
public:
	virtual ~STRasterCacheStructure();
protected:
	virtual std::unique_ptr<GenericRaster> copy(const GenericRaster &content) const;
	virtual std::unique_ptr<STEntryBounds> create_bounds(const GenericRaster &content) const;
	virtual uint64_t get_content_size(const GenericRaster &content) const;
};


STRasterCacheStructure::~STRasterCacheStructure() {
	// Nothing to-do
}

std::unique_ptr<GenericRaster> STRasterCacheStructure::copy(
		const GenericRaster& content) const {
	auto copy = GenericRaster::create(content.dd, content, content.getRepresentation());
	copy->blit(&content, 0);
	return copy;
}

std::unique_ptr<STEntryBounds> STRasterCacheStructure::create_bounds(
		const GenericRaster& content) const {
	return make_unique<STRasterEntryBounds>(content);
}

uint64_t STRasterCacheStructure::get_content_size(const GenericRaster& content) const {
	// TODO: Get correct size
	return sizeof(content) + content.getDataSize();
}


///////////////////////////////////////////////////////////////////
//
// RASTER-REFERENCE CACHE-STRUCTURE (FOR USE IN INDEX)
//
///////////////////////////////////////////////////////////////////

class RasterRefStructure : public STCacheStructure<STRasterRef> {
public:
	RasterRefStructure() : next_id(1) {};
	virtual ~RasterRefStructure() {};
	virtual uint64_t insert(const std::unique_ptr<STRasterRef> &result);
	virtual std::unique_ptr<STRasterRef> get_copy(const uint64_t id) const;
	virtual const std::shared_ptr<STRasterRef> get(const uint64_t id) const;
	virtual uint64_t get_entry_size(const uint64_t id) const;
	virtual void remove(const uint64_t id);
protected:
	virtual std::unique_ptr<std::priority_queue<STQueryInfo>> get_query_candidates( const QueryRectangle &spec ) const;
private:
	std::unordered_map<uint64_t, std::shared_ptr<STRasterRef>> entries;
	uint64_t next_id;
};

uint64_t RasterRefStructure::insert(const std::unique_ptr<STRasterRef>& result) {
	uint64_t id = next_id++;
	Log::trace("Inserting new reference-entry. Id: %d, bounds: %s", id, result->bounds.to_string().c_str() );
	entries.emplace(id,std::shared_ptr<STRasterRef>( new STRasterRef(*result) ) );
	return id;
}

const std::shared_ptr<STRasterRef> RasterRefStructure::get(const uint64_t id) const {
	Log::trace("Retrieving cache-reference with id: %d", id);
	try {
		return entries.at(id);
	} catch ( std::out_of_range &oor ) {
		std::ostringstream msg;
		msg << "No cache-entry with id: " << id;
		throw NoSuchElementException(msg.str());
	}
}

std::unique_ptr<STRasterRef> RasterRefStructure::get_copy(const uint64_t id) const {
	Log::trace("Returning copy of reference with id: %d", id);
	return make_unique<STRasterRef>( *get(id) );
}

uint64_t RasterRefStructure::get_entry_size(const uint64_t id) const {
	(void) id;
	return 0;
}

void RasterRefStructure::remove(const uint64_t id) {
	Log::trace("Removing reference with id: %d", id);
	entries.erase(id);
}

inline std::unique_ptr<std::priority_queue<STQueryInfo> > RasterRefStructure::get_query_candidates(
		const QueryRectangle& spec) const {
	Log::trace("Fetching candidates for query: %s", CacheCommon::qr_to_string(spec).c_str() );
	std::unique_ptr<std::priority_queue<STQueryInfo>> partials = make_unique<std::priority_queue<STQueryInfo>>();
	for (auto &e : entries) {
		const STEntryBounds &bounds = e.second->bounds;
		double coverage = bounds.get_coverage(spec);
		Log::trace("Coverage for reference %d: %f", e.first, coverage);
		if ( coverage > 0 )
			partials->push(STQueryInfo( coverage, bounds.x1, bounds.x2, bounds.y1, bounds.y2, e.first ));
	}
	Log::trace("Found %d candidates for query: %s", partials->size(), CacheCommon::qr_to_string(spec).c_str() );
	return partials;

}

///////////////////////////////////////////////////////////////////
//
// BASIC CACHE-IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

template<typename EType>
STCache<EType>::STCache(size_t max_size) : max_size(max_size), current_size(0) {
	Log::debug("Creating new cache with max-size: %d", max_size);
	//policy = std::unique_ptr<ReplacementPolicy<EType>>( new LRUPolicy<EType>() );
}

template<typename EType>
STCache<EType>::~STCache() {
	for (auto &entry : caches) {
		delete entry.second;
	}
}

template<typename EType>
STCacheStructure<EType>* STCache<EType>::get_structure(const std::string &key, bool create) const {

	Log::trace("Retrieving cache-structure for semantic_id: %s", key.c_str() );

	STCacheStructure<EType> *cache;
	auto got = caches.find(key);

	if (got == caches.end() && create) {
		Log::trace("No cache-structure found for semantic_id: %s. Creating.", key.c_str() );
		cache = new_structure();
		caches.emplace(key,cache);
	}
	else if (got != caches.end())
		cache = got->second;
	else
		cache = nullptr;
	return cache;
}

template<typename EType>
void STCache<EType>::remove(const STCacheKey& key) {
	remove( key.semantic_id, key.entry_id );
}

template<typename EType>
void STCache<EType>::remove(const std::string& semantic_id, uint64_t id) {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(semantic_id);
	if (cache != nullptr) {
		try {
			auto size = cache->get_entry_size(id);
			current_size -= size;
			cache->remove(id);
		} catch ( NoSuchElementException &nse ) {
		}
	}
}

template<typename EType>
STCacheKey STCache<EType>::put(const std::string &key, const std::unique_ptr<EType> &item) {
	std::lock_guard<std::mutex> lock(mtx);

	auto cache = get_structure(key, true);
	auto id = cache->insert(item);
	current_size += cache->get_entry_size(id);
	return STCacheKey(key,id);
}

template<typename EType>
const std::shared_ptr<EType> STCache<EType>::get(const STCacheKey& key) const {
	return get(key.semantic_id, key.entry_id);
}

template<typename EType>
const std::shared_ptr<EType> STCache<EType>::get(const std::string& semantic_id, uint64_t id) const {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(semantic_id);
	if (cache != nullptr)
		return cache->get(id);
	else
		throw NoSuchElementException("Entry not found");
}

template<typename EType>
std::unique_ptr<EType> STCache<EType>::get_copy(const STCacheKey& key) const {
	return get_copy(key.semantic_id, key.entry_id);
}

template<typename EType>
std::unique_ptr<EType> STCache<EType>::get_copy(const std::string& semantic_id, uint64_t id) const {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(semantic_id);
	if (cache != nullptr)
		return cache->get_copy(id);
	else
		throw NoSuchElementException("Entry not found");
}

template<typename EType>
STQueryResult STCache<EType>::query(const std::string& semantic_id, const QueryRectangle& qr) const {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(semantic_id);
	if (cache != nullptr)
		return cache->query(qr);
	else {
		return STQueryResult(qr);
	}
}

///////////////////////////////////////////////////////////////////
//
// RASTER CACHE-IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

RasterCache::RasterCache(size_t size) : STCache(size) {
}

RasterCache::~RasterCache() {
}

STCacheStructure<GenericRaster>* RasterCache::new_structure() const {
	return new STRasterCacheStructure();
}

///////////////////////////////////////////////////////////////////
//
// RASTER-REFERENCE CACHE-IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

RasterRefCache::RasterRefCache() : STCache( (size_t) 1 << 63) {
}

RasterRefCache::~RasterRefCache() {
}

STCacheStructure<STRasterRef>* RasterRefCache::new_structure() const {
	return new RasterRefStructure();
}

//
// We have to manage our *_by_node structure here
//

STCacheKey RasterRefCache::put(const std::string& semantic_id, const std::unique_ptr<STRasterRef>& item) {
	STCacheKey key = STCache<STRasterRef>::put(semantic_id,item);
	try {
		auto &v = entries_by_node.at(item->node_id);
		v.push_back( key );
	} catch ( std::out_of_range &oor ) {
		std::vector<STCacheKey> vec;
		vec.push_back(key);
		entries_by_node[item->node_id] = vec;
	}
	return key;
}

void RasterRefCache::remove(const std::string& semantic_id, uint64_t id) {
	try {
		auto &v = entries_by_node.at(get(semantic_id,id)->node_id);
		auto e_it = v.begin();
		while ( e_it != v.end() ) {
			if ( e_it->semantic_id == semantic_id &&
				 e_it->entry_id == id )
				e_it = v.erase(e_it);
			else
				++e_it;
		}
		STCache<STRasterRef>::remove(semantic_id,id);
	} catch ( NoSuchElementException &nse ) {
		// Nothing to-do here
	}
}

void RasterRefCache::remove_all_by_node(uint32_t node_id) {
	try {
		for ( auto &k : entries_by_node.at(node_id) )
			STCache<STRasterRef>::remove( k.semantic_id, k.entry_id );
	} catch ( std::out_of_range &oor ) {

	}
	entries_by_node.erase(node_id);
}

// Instantiate all
template class STCache<GenericRaster> ;
template class STCache<STRasterRef>;
