/*
 * index_cache.cpp
 *
 *  Created on: 07.08.2015
 *      Author: mika
 */

#include "cache/index/index_cache.h"
#include "cache/index/reorg_strategy.h"
#include "util/exceptions.h"
#include "util/log.h"

//
// Index key
//

IndexCacheKey::IndexCacheKey(const std::string& semantic_id, std::pair<uint32_t, uint64_t> id) :
	NodeCacheKey(semantic_id, id.second), node_id(id.first){
}

IndexCacheKey::IndexCacheKey(uint32_t node_id, const std::string &semantic_id, uint64_t entry_id) :
	NodeCacheKey(semantic_id,entry_id), node_id(node_id) {
}

std::string IndexCacheKey::to_string() const {
	return concat( "NodeCacheKey[ semantic_id: ", semantic_id, ", id: ", entry_id, ", node: ", node_id, "]");
}

//
// Index entry
//
IndexCacheEntry::IndexCacheEntry(uint32_t node_id, const NodeCacheRef &ref) :
	IndexCacheKey(node_id, ref.semantic_id,ref.entry_id), CacheEntry(ref) {
}

//////////////////////////////////////////////////////////////////
//
// INDEX-CACHE
//
//////////////////////////////////////////////////////////////////

IndexCache::IndexCache(const std::string &reorg_strategy) :
	reorg_strategy(std::move(ReorgStrategy::by_name(*this,reorg_strategy))) {
}


void IndexCache::put( const IndexCacheEntry& entry) {
	auto cache = get_structure(entry.semantic_id,true);
	//FIXME: This sucks -- extra copy
	std::shared_ptr<IndexCacheEntry> e( new IndexCacheEntry(entry) );
	std::pair<uint32_t,uint64_t> id(entry.node_id,entry.entry_id);
	cache->put(id, e);
	get_node_entries(entry.node_id).push_back(e);
}

std::shared_ptr<const IndexCacheEntry> IndexCache::get(const IndexCacheKey& key) const {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		std::pair<uint32_t,uint64_t> id(key.node_id,key.entry_id);
		return cache->get(id);
	}
	throw NoSuchElementException("Entry not found");
}

CacheQueryResult<std::pair<uint32_t,uint64_t>> IndexCache::query(const std::string& semantic_id, const QueryRectangle& qr) const {
	ExecTimer t("IndexCache.query");
	auto cache = get_structure(semantic_id);
	if (cache != nullptr)
		return cache->query(qr);
	else {
		return CacheQueryResult<std::pair<uint32_t,uint64_t>>(qr);
	}
}

void IndexCache::remove(const IndexCacheKey& key) {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		try {
			std::pair<uint32_t,uint64_t> id(key.node_id,key.entry_id);
			auto entry = cache->remove(id);
			remove_from_node(key);
		} catch ( const NoSuchElementException &nse ) {
			Log::warn("Removal of index-entry failed: %s", nse.what());
		}
	}
	else
		Log::warn("Removal of index-entry failed. No structure for semantic_id: %s", key.semantic_id.c_str());
}

void IndexCache::move(const IndexCacheKey& old_key, const IndexCacheKey& new_key) {
	auto cache = get_structure(old_key.semantic_id);
	if (cache != nullptr) {
		std::pair<uint32_t,uint64_t> id(old_key.node_id,old_key.entry_id);
		auto entry = cache->remove(id);
		remove_from_node(old_key);

		id.first = new_key.node_id;
		id.second = new_key.entry_id;
		entry->node_id = new_key.node_id;
		entry->entry_id = new_key.entry_id;

		cache->put(id,entry);
		get_node_entries(new_key.node_id).push_back(entry);
	}
	else
		throw NoSuchElementException("Entry not found");
}

void IndexCache::remove_all_by_node(uint32_t node_id) {
	auto entries = get_node_entries(node_id);
	for ( auto &key : entries ) {
		remove(*key);
	}
	entries_by_node.erase(node_id);
}

CacheStructure<std::pair<uint32_t,uint64_t>,IndexCacheEntry>* IndexCache::get_structure(const std::string& semantic_id, bool create) const {
	Log::trace("Retrieving cache-structure for semantic_id: %s", semantic_id.c_str() );
	Struct *cache;
	auto got = caches.find(semantic_id);
	if (got == caches.end() && create) {
		Log::trace("No cache-structure found for semantic_id: %s. Creating.", semantic_id.c_str() );
		cache = new Struct();
		caches.emplace(semantic_id,cache);
	}
	else if (got != caches.end())
		cache = got->second;
	else
		cache = nullptr;
	return cache;
}

std::vector<std::shared_ptr<IndexCacheEntry>>& IndexCache::get_node_entries(uint32_t node_id) const {
	try {
		return entries_by_node.at(node_id);
	} catch ( const std::out_of_range &oor ) {
		entries_by_node.emplace( node_id, std::vector<std::shared_ptr<IndexCacheEntry>>() );
		return entries_by_node.at(node_id);
	}
}

double IndexCache::get_capacity_usage(const Capacity& capacity) const {
	if ( get_total_capacity(capacity) > 0 )
		return (double) get_used_capacity(capacity) / (double) get_total_capacity(capacity);
	return 1;
}

void IndexCache::remove_from_node(const IndexCacheKey& key) {
	auto &entries = get_node_entries(key.node_id);
	auto iter = entries.begin();
	while ( iter != entries.end() ) {
		if ( (*iter)->semantic_id == key.semantic_id &&
			 (*iter)->entry_id == key.entry_id ) {
			iter = entries.erase(iter);
			return;
		}
		iter++;
	}
	throw NoSuchElementException("Entry not found in node-list.");
}


uint32_t IndexCache::get_node_for_job(const QueryRectangle& query, const std::map<uint32_t,std::shared_ptr<Node>> &nodes) const {
	return reorg_strategy->get_node_for_job(query, nodes);
}

bool IndexCache::requires_reorg( const std::map<uint32_t, std::shared_ptr<Node> > &nodes ) const {
	return reorg_strategy->requires_reorg(nodes);
}

void IndexCache::reorganize(
	std::map<uint32_t, NodeReorgDescription>& result) {
	reorg_strategy->reorganize(result);
}

void IndexCache::update_stats(uint32_t node_id, const CacheStats &stats) {
	for ( auto &kv : stats.get_stats() ) {
		auto cache = get_structure(kv.first);
		if ( cache == nullptr )
			continue;
		std::pair<uint32_t,uint64_t> id(node_id,0);
		for ( auto &s : kv.second ) {
			id.second = s.entry_id;
			auto e = cache->get(id);
			e->access_count = s.access_count;
			e->last_access = s.last_access;
		}
	}
}

//
// Raster-Cache
//

IndexRasterCache::IndexRasterCache(const std::string &reorg_strategy) : IndexCache(reorg_strategy) {
}

size_t IndexRasterCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.raster_cache_total;
}

size_t IndexRasterCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.raster_cache_used;
}

CacheType IndexRasterCache::get_reorg_type() const {
	return CacheType::RASTER;
}

IndexPointCache::IndexPointCache(const std::string& reorg_strategy) : IndexCache(reorg_strategy) {
}

size_t IndexPointCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.point_cache_total;
}

size_t IndexPointCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.point_cache_used;
}

CacheType IndexPointCache::get_reorg_type() const {
	return CacheType::POINT;
}

IndexLineCache::IndexLineCache(const std::string& reorg_strategy) : IndexCache(reorg_strategy) {
}

size_t IndexLineCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.line_cache_total;
}

size_t IndexLineCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.line_cache_used;
}

CacheType IndexLineCache::get_reorg_type() const {
	return CacheType::LINE;
}

IndexPolygonCache::IndexPolygonCache(const std::string& reorg_strategy) : IndexCache(reorg_strategy) {
}

size_t IndexPolygonCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.polygon_cache_total;
}

size_t IndexPolygonCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.point_cache_used;
}

CacheType IndexPolygonCache::get_reorg_type() const {
	return CacheType::POLYGON;
}

IndexPlotCache::IndexPlotCache(const std::string& reorg_strategy) : IndexCache(reorg_strategy) {
}

size_t IndexPlotCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.plot_cache_total;
}

size_t IndexPlotCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.plot_cache_used;
}

CacheType IndexPlotCache::get_reorg_type() const {
	return CacheType::PLOT;
}

//
//
//

IndexCaches::IndexCaches(const std::string& reorg_strategy) :
	raster_cache(reorg_strategy),
	point_cache(reorg_strategy),
	line_cache(reorg_strategy),
	poly_cache(reorg_strategy),
	plot_cache(reorg_strategy){

	all_caches.push_back(raster_cache);
	all_caches.push_back(point_cache);
	all_caches.push_back(line_cache);
	all_caches.push_back(poly_cache);
	all_caches.push_back(plot_cache);
}

IndexCache& IndexCaches::get_cache(CacheType type) {
	switch ( type ) {
		case CacheType::RASTER:
			return raster_cache;
		case CacheType::POINT:
			return point_cache;
		case CacheType::LINE:
			return line_cache;
		case CacheType::POLYGON:
			return poly_cache;
		case CacheType::PLOT:
			return plot_cache;
		default:
			throw ArgumentException(concat("Unknown cache-type: ",(int)type));
	}
}

bool IndexCaches::require_reorg(const std::map<uint32_t, std::shared_ptr<Node> > &nodes) const {
	for ( IndexCache &c : all_caches )
		if ( c.requires_reorg(nodes) )
			return true;
	return false;
}

void IndexCaches::remove_all_by_node(uint32_t node_id) {
	for ( IndexCache &c : all_caches )
		c.remove_all_by_node(node_id);
}

void IndexCaches::process_handshake(uint32_t node_id, const NodeHandshake& hs) {
	for (auto &e : hs.get_entries())
		get_cache(e.type).put( IndexCacheEntry(node_id,e) );
}

void IndexCaches::update_stats(uint32_t node_id, const NodeStats& stats) {
	for ( auto &s : stats.stats ) {
		get_cache(s.type).update_stats(node_id,s);
	}
}

void IndexCaches::reorganize(const std::map<uint32_t, std::shared_ptr<Node> > &nodes, std::map<uint32_t, NodeReorgDescription>& result) {
	Log::info("Calculating reorganization of cache");
	for ( IndexCache &c : all_caches ) {
		if ( c.requires_reorg(nodes) )
			c.reorganize(result);
	}
	Log::info("Finished calculating reorganization of cache");
}
