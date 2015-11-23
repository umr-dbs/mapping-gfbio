/*
 * node_cache.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/node/node_cache.h"
#include "util/log.h"
#include "util/make_unique.h"

//////////////////////////////////////////////////////////////
//
// Value objects
//
//////////////////////////////////////////////////////////////

template<typename EType>
NodeCacheEntry<EType>::NodeCacheEntry(uint64_t entry_id, std::shared_ptr<EType> result,
	uint64_t size) : CacheEntry( CacheCube(*result), size ), entry_id(entry_id), data(result) {
}

template<typename EType>
std::string NodeCacheEntry<EType>::to_string() const {
	return concat("CacheEntry[id: ", entry_id, ", size: ", size, ", last_access: ", last_access, ", access_count: ", access_count, ", bounds: ", bounds.to_string(), "]");
}

///////////////////////////////////////////////////////////////////
//
// NODE-CACHE IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

template<typename EType>
NodeCache<EType>::NodeCache(CacheType type, size_t max_size) : type(type), max_size(max_size), current_size(0), next_id(1) {
	Log::debug("Creating new cache with capacity: %d bytes", max_size);
}

template<typename EType>
NodeCache<EType>::~NodeCache() {
	for (auto &entry : caches) {
		delete entry.second;
	}
}

template<typename EType>
std::shared_ptr<NodeCacheEntry<EType> > NodeCache<EType>::create_entry(uint64_t id, const EType& data) {

	auto cpy = copy( data );
	size_t size = get_data_size(data);

	return std::shared_ptr<NodeCacheEntry<EType>>( new NodeCacheEntry<EType>(id, std::shared_ptr<EType>(cpy.release()), size) );
}

template<typename EType>
std::vector<NodeCacheRef> NodeCache<EType>::get_all() const {
	std::lock_guard<std::mutex> guard(mtx);

	std::vector<NodeCacheRef> result;
	for ( auto &ce : caches )
		for ( auto &ne : ce.second->get_all() )
			result.push_back( NodeCacheRef(type,ce.first, ne->entry_id, *ne) );
	return result;
}

template<typename EType>
CacheStructure<uint64_t,NodeCacheEntry<EType>>* NodeCache<EType>::get_structure(const std::string &key, bool create) const {
	std::lock_guard<std::mutex> guard(mtx);
	Log::trace("Retrieving cache-structure for semantic_id: %s", key.c_str() );
	Struct *cache;
	auto got = caches.find(key);
	if (got == caches.end() && create) {
		Log::trace("No cache-structure found for semantic_id: %s. Creating.", key.c_str() );
		cache = new Struct();
		caches.emplace(key,cache);
	}
	else if (got != caches.end())
		cache = got->second;
	else
		cache = nullptr;
	return cache;
}

template<typename EType>
void NodeCache<EType>::remove(const NodeCacheKey& key) {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		try {
			auto entry = cache->remove(key.entry_id);
			current_size -= entry->size;
		} catch ( const NoSuchElementException &nse ) {
			Log::warn("Item could not be removed: %s", key.to_string().c_str());
		}
	}
}

template<typename EType>
const NodeCacheRef NodeCache<EType>::put(const std::string &semantic_id, const std::unique_ptr<EType> &item, const AccessInfo info) {
	uint64_t id = next_id++;
	auto cache = get_structure(semantic_id, true);
	auto entry = create_entry( id, *item  );
	entry->last_access = info.last_access;
	entry->access_count = info.access_count;
	cache->put( id, entry );
	current_size += entry->size;
	return NodeCacheRef( type, semantic_id, entry->entry_id, *entry );
}

template<typename EType>
const std::shared_ptr<EType> NodeCache<EType>::get(const NodeCacheKey& key) const {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		std::shared_ptr<NodeCacheEntry<EType>> e = cache->get(key.entry_id);
		track_access( key, *e );
		return e->data;
	}
	else
		throw NoSuchElementException("Entry not found");
}


template<typename EType>
std::unique_ptr<EType> NodeCache<EType>::get_copy(const NodeCacheKey& key) const {
	return copy( *get(key) );
}

template<typename EType>
CacheQueryResult<uint64_t> NodeCache<EType>::query(const std::string& semantic_id, const QueryRectangle& qr) const {
	auto cache = get_structure(semantic_id);
	if (cache != nullptr)
		return cache->query(qr);
	else {
		return CacheQueryResult<uint64_t>(qr);
	}
}

template<typename EType>
const NodeCacheRef NodeCache<EType>::get_entry_metadata(const NodeCacheKey& key) const {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		auto e = cache->get( key.entry_id );
		return NodeCacheRef( type, key, *e );
	}
	else
		throw NoSuchElementException("Entry not found");
}

template<typename EType>
CacheStats NodeCache<EType>::get_stats() const {
	std::lock_guard<std::mutex> g(access_mtx);
	CacheStats result(type);
	for ( auto &kv : access_tracker ) {
		auto cache = get_structure( kv.first );
		if ( cache == nullptr )
			continue;

		for ( auto &id : kv.second ) {
			auto e = cache->get( id );
			result.add_stats( kv.first, NodeEntryStats( id, e->last_access, e->access_count ) );
		}
	}
	access_tracker.clear();
	return result;
}

template<typename EType>
void NodeCache<EType>::track_access(const NodeCacheKey& key, NodeCacheEntry<EType> &e) const {
	std::lock_guard<std::mutex> g(access_mtx);
	e.access_count++;
	time(&(e.last_access));
	try {
		access_tracker.at(key.semantic_id).insert(key.entry_id);
	} catch ( const std::out_of_range &oor ) {
		std::set<uint64_t> ids;
		ids.insert(key.entry_id);
		access_tracker.emplace( key.semantic_id, ids );
	}
}

///////////////////////////////////////////////////////////////////
//
// RASTER CACHE-IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

NodeRasterCache::NodeRasterCache(size_t size) : NodeCache<GenericRaster>(CacheType::RASTER,size) {
}

std::unique_ptr<GenericRaster> NodeRasterCache::copy(const GenericRaster& content) const {
	auto copy = GenericRaster::create(content.dd, content, content.getRepresentation());
	copy->blit(&content, 0);
	copy->setRepresentation(GenericRaster::Representation::CPU);
	return copy;
}

size_t NodeRasterCache::get_data_size(const GenericRaster& content) const {
	return sizeof(content) + content.getDataSize();
}

NodePointCache::NodePointCache(size_t size) : NodeCache<PointCollection>(CacheType::POINT,size) {
}

std::unique_ptr<PointCollection> NodePointCache::copy(const PointCollection& content) const {
	return make_unique<PointCollection>( content );
}

size_t NodePointCache::get_data_size(const PointCollection& content) const {
	// TODO
	return content.getFeatureCount();
}

NodeLineCache::NodeLineCache(size_t size) : NodeCache<LineCollection>(CacheType::LINE,size) {
}

std::unique_ptr<LineCollection> NodeLineCache::copy(const LineCollection& content) const {
	return make_unique<LineCollection>( content );
}

size_t NodeLineCache::get_data_size(const LineCollection& content) const {
	// TODO
	return content.getFeatureCount();
}

NodePolygonCache::NodePolygonCache(size_t size) : NodeCache<PolygonCollection>(CacheType::POLYGON,size) {
}

std::unique_ptr<PolygonCollection> NodePolygonCache::copy(const PolygonCollection& content) const {
	return make_unique<PolygonCollection>( content );
}

size_t NodePolygonCache::get_data_size(const PolygonCollection& content) const {
	// TODO
	return content.getFeatureCount();
}

NodePlotCache::NodePlotCache(size_t size) : NodeCache<GenericPlot>(CacheType::PLOT,size) {
}

std::unique_ptr<GenericPlot> NodePlotCache::copy(const GenericPlot& content) const {
	// TODO
	(void) content;
	return std::unique_ptr<GenericPlot>();
}

size_t NodePlotCache::get_data_size(const GenericPlot& content) const {
	// TODO
	(void) content;
	return 1;
}

// Instantiate all
template class NodeCache<GenericRaster>;
template class NodeCache<PointCollection>;
template class NodeCache<LineCollection>;
template class NodeCache<PolygonCollection>;
template class NodeCache<GenericPlot>;
