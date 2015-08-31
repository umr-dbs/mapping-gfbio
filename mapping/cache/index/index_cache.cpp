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
IndexCacheEntry::IndexCacheEntry(uint32_t node_id, NodeCacheRef ref) :
	IndexCacheKey(node_id, ref.semantic_id,ref.entry_id), CacheEntry(ref) {
}

//////////////////////////////////////////////////////////////////
//
// INDEX-CACHE
//
//////////////////////////////////////////////////////////////////

IndexCache::IndexCache(ReorgStrategy& strategy) : reorg_strategy(strategy) {
}


IndexCache::~IndexCache() {
}

void IndexCache::put( const IndexCacheEntry& entry) {
	auto cache = get_structure(entry.semantic_id,true);
	//FIXME: This sucks -- extra copy
	std::shared_ptr<IndexCacheEntry> e( new IndexCacheEntry(entry) );
	std::pair<uint32_t,uint64_t> id(entry.node_id,entry.entry_id);
	cache->put(id, e);
	get_node_entries(entry.node_id).push_back(e);
}

const IndexCacheEntry& IndexCache::get(const IndexCacheKey& key) const {
	auto cache = get_structure(key.semantic_id);
	if (cache != nullptr) {
		std::pair<uint32_t,uint64_t> id(key.node_id,key.entry_id);
		return *cache->get(id);
	}
	throw NoSuchElementException("Entry not found");
}

CacheQueryResult<std::pair<uint32_t,uint64_t>> IndexCache::query(const std::string& semantic_id, const QueryRectangle& qr) const {
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
		Log::warn("Removal of index-entry failed. No structure for semantic_id: %s", key.semantic_id);
}

void IndexCache::move(const IndexCacheKey& old_key, const IndexCacheKey& new_key) {
	auto cache = get_structure(old_key.semantic_id);
	if (cache != nullptr) {
		std::pair<uint32_t,uint64_t> id(old_key.node_id,old_key.entry_id);
		auto entry = cache->remove(id);

		id.first = new_key.node_id;
		id.second = new_key.entry_id;
		entry->node_id = new_key.node_id;
		entry->entry_id = new_key.entry_id;
		cache->put(id,entry);

		remove_from_node(old_key);
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

bool IndexCache::requires_reorg( const std::map<uint32_t, std::shared_ptr<Node> > &nodes ) {
	return reorg_strategy.requires_reorg(*this,nodes);
}

void IndexCache::reorganize(
	std::map<uint32_t, NodeReorgDescription>& result) {
	reorg_strategy.reorganize(*this,result);
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

IndexRasterCache::IndexRasterCache(ReorgStrategy& strategy) : IndexCache(strategy) {
}

IndexRasterCache::~IndexRasterCache() {
}

size_t IndexRasterCache::get_total_capacity(const Capacity& capacity) const {
	return capacity.raster_cache_total;
}

size_t IndexRasterCache::get_used_capacity(const Capacity& capacity) const {
	return capacity.raster_cache_used;
}

double IndexRasterCache::get_capacity_usage(const Capacity& capacity) const {
	return capacity.get_raster_usage();
}

ReorgRemoveItem::Type IndexRasterCache::get_reorg_type() const {
	return ReorgRemoveItem::Type::RASTER;
}
