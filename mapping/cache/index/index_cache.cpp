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

IndexCache::IndexCache(const std::string &reorg_strategy, const std::string &relevance_function) :
	reorg_strategy(ReorgStrategy::by_name(*this,reorg_strategy,relevance_function)) {
}


void IndexCache::put( const std::shared_ptr<IndexCacheEntry>& entry) {
	std::pair<uint32_t,uint64_t> id(entry->node_id,entry->entry_id);
	this->put_int(entry->semantic_id,id,entry);
	get_node_entries(entry->node_id).insert(entry);
}

std::shared_ptr<const IndexCacheEntry> IndexCache::get(const IndexCacheKey& key) const {
	std::pair<uint32_t,uint64_t> id(key.node_id,key.entry_id);
	return this->get_int(key.semantic_id,id);
}

void IndexCache::remove(const IndexCacheKey& key) {
	try {
		std::pair<uint32_t,uint64_t> id(key.node_id,key.entry_id);
		auto e = this->remove_int(key.semantic_id,id);
		remove_from_node(e);
	} catch ( const NoSuchElementException &nse ) {
		Log::warn("Removal of index-entry failed: %s", nse.what());
	}
}

void IndexCache::move(const IndexCacheKey& old_key, const IndexCacheKey& new_key) {
	std::pair<uint32_t,uint64_t> id(old_key.node_id,old_key.entry_id);
	auto entry = this->remove_int(old_key.semantic_id,id);
	remove_from_node(entry);
	id.first = new_key.node_id;
	id.second = new_key.entry_id;
	entry->node_id = new_key.node_id;
	entry->entry_id = new_key.entry_id;
	put_int(new_key.semantic_id,id,entry);
	get_node_entries(new_key.node_id).insert(entry);
}

void IndexCache::remove_all_by_node(uint32_t node_id) {
	auto entries = get_node_entries(node_id);
	for ( auto &key : entries ) {
		this->remove_int(key->semantic_id,std::pair<uint32_t,uint64_t>(key->node_id,key->entry_id));
	}
	entries_by_node.erase(node_id);
}

std::vector<std::shared_ptr<const IndexCacheEntry> > IndexCache::get_all() const {
	std::vector<std::shared_ptr<const IndexCacheEntry>> result;
	size_t size = 0;
	for ( auto &p : entries_by_node )
		size += p.second.size();

	result.reserve(size);
	for ( auto &p : entries_by_node )
		result.insert(result.end(), p.second.begin(), p.second.end());

	return result;
}

std::set<std::shared_ptr<const IndexCacheEntry>>& IndexCache::get_node_entries(uint32_t node_id) const {
	try {
		return entries_by_node.at(node_id);
	} catch ( const std::out_of_range &oor ) {
		return entries_by_node.emplace( node_id, std::set<std::shared_ptr<const IndexCacheEntry>>() ).first->second;
	}
}

double IndexCache::get_capacity_usage(const Capacity& capacity) const {
	if ( get_total_capacity(capacity) > 0 )
		return (double) get_used_capacity(capacity) / (double) get_total_capacity(capacity);
	return 1;
}

void IndexCache::remove_from_node(const std::shared_ptr<IndexCacheEntry> &e ) {
	if ( get_node_entries(e->node_id).erase(e) != 1 )
		throw NoSuchElementException("Entry not found in node-list.");
}


uint32_t IndexCache::get_node_for_job(const BaseRequest& request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes) const {
	return reorg_strategy->get_node_for_job(request, nodes);
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
		std::pair<uint32_t,uint64_t> id(node_id,0);
		for ( auto &s : kv.second ) {
			id.second = s.entry_id;
			try {
				auto e = get_int(kv.first,id);
				e->access_count = s.access_count;
				e->last_access = s.last_access;
			}
			catch ( const NoSuchElementException &nse ) {
				// Nothing TO DO
			}
		}
	}
}

//
// Raster-Cache
//

IndexRasterCache::IndexRasterCache(const std::string &reorg_strategy, const std::string &relevance_function)
	: IndexCache(reorg_strategy,relevance_function) {
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

IndexPointCache::IndexPointCache(const std::string& reorg_strategy, const std::string &relevance_function)
	: IndexCache(reorg_strategy, relevance_function) {
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

IndexLineCache::IndexLineCache(const std::string& reorg_strategy, const std::string &relevance_function)
	: IndexCache(reorg_strategy,relevance_function) {
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

IndexPolygonCache::IndexPolygonCache(const std::string& reorg_strategy, const std::string &relevance_function)
	: IndexCache(reorg_strategy,relevance_function) {
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

IndexPlotCache::IndexPlotCache(const std::string& reorg_strategy, const std::string &relevance_function)
	: IndexCache(reorg_strategy,relevance_function) {
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

IndexCaches::IndexCaches(const std::string& reorg_strategy, const std::string &relevance_function) :
	raster_cache(reorg_strategy,relevance_function),
	point_cache(reorg_strategy,relevance_function),
	line_cache(reorg_strategy,relevance_function),
	poly_cache(reorg_strategy,relevance_function),
	plot_cache(reorg_strategy,relevance_function){

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
		get_cache(e.type).put( std::shared_ptr<IndexCacheEntry>( new IndexCacheEntry(node_id,e) ) );
}

void IndexCaches::update_stats(uint32_t node_id, const NodeStats& stats) {
	for ( auto &s : stats.stats ) {
		get_cache(s.type).update_stats(node_id,s);
	}
}

void IndexCaches::reorganize(const std::map<uint32_t, std::shared_ptr<Node> > &nodes, std::map<uint32_t, NodeReorgDescription>& result, bool force) {
	Log::info("Calculating reorganization of cache");
	for ( IndexCache &c : all_caches ) {
		if ( c.requires_reorg(nodes) || force )
			c.reorganize(result);
	}
	Log::info("Finished calculating reorganization of cache");
}
