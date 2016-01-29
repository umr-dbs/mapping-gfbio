/*
 * node_cache.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/node/node_cache.h"
#include "util/log.h"
#include "util/make_unique.h"
#include "util/sizeutil.h"

#include <cstring>

//////////////////////////////////////////////////////////////
//
// Value objects
//
//////////////////////////////////////////////////////////////

template<typename EType>
NodeCacheEntry<EType>::NodeCacheEntry(uint64_t entry_id, const CacheEntry &meta,
		std::shared_ptr<EType> result) :
		CacheEntry(meta), entry_id(entry_id), data(result) {
}

template<typename EType>
std::unique_ptr<EType> NodeCacheEntry<EType>::copy_data() const {
	return data->clone();
}

template<>
std::unique_ptr<GenericRaster> NodeCacheEntry<GenericRaster>::copy_data() const {
	return const_cast<GenericRaster*>(data.get())->clone();
}


template<typename EType>
std::string NodeCacheEntry<EType>::to_string() const {
	return concat("CacheEntry[id: ", entry_id, ", size: ", size,
			", last_access: ", last_access, ", access_count: ", access_count,
			", bounds: ", bounds.to_string(), "]");
}

template class NodeCacheEntry<GenericRaster>;
template class NodeCacheEntry<PointCollection>;
template class NodeCacheEntry<LineCollection>;
template class NodeCacheEntry<PolygonCollection>;
template class NodeCacheEntry<GenericPlot>;

///////////////////////////////////////////////////////////////////
//
// NODE-CACHE IMPLEMENTATION
//
///////////////////////////////////////////////////////////////////

template<typename EType>
NodeCache<EType>::NodeCache(CacheType type, size_t max_size) :
		type(type), max_size(max_size), current_size(0), next_id(1) {
	Log::debug("Creating new cache with capacity: %d bytes", max_size);
}

template<typename EType>
std::vector<NodeCacheRef> NodeCache<EType>::get_all() const {
	std::vector<NodeCacheRef> result;
	size_t size = 0;
	auto all_int = this->get_all_int();
	for (auto &p : all_int)
		size += p.second.size();

	result.reserve(size);
	for (auto &p : all_int)
		for (auto &ne : p.second)
			result.push_back(NodeCacheRef(type, p.first, ne->entry_id, *ne));
	return result;
}

template<typename EType>
void NodeCache<EType>::remove(const NodeCacheKey& key) {
	try {
		auto entry = this->remove_int(key.semantic_id, key.entry_id);
		current_size -= entry->size;
	} catch (const NoSuchElementException &nse) {
		Log::warn("Item could not be removed: %s", key.to_string().c_str());
	}
}

template<typename EType>
const NodeCacheRef NodeCache<EType>::put(const std::string &semantic_id,
		const std::unique_ptr<EType> &item, const CacheEntry &meta) {
	uint64_t id = next_id++;
	auto cpy = const_cast<EType*>(item.get())->clone();
	auto entry = std::shared_ptr<NodeCacheEntry<EType>>(
			new NodeCacheEntry<EType>(id, meta,
					std::shared_ptr<EType>(cpy.release())));
	this->put_int(semantic_id, id, entry);
	current_size += entry->size;
	return NodeCacheRef(type, semantic_id, id, *entry);
}

template<typename EType>
std::shared_ptr<const NodeCacheEntry<EType>> NodeCache<EType>::get( const NodeCacheKey &key ) const {
	auto res = this->get_int(key.semantic_id, key.entry_id);
	track_access(key, *res);
	return res;
}

//template<typename EType>
//const std::shared_ptr<const EType> NodeCache<EType>::get(
//		const NodeCacheKey& key) const {
//	auto res = this->get_int(key.semantic_id, key.entry_id);
//	track_access(key, *res);
//	return res->data;
//}
//
//template<typename EType>
//std::unique_ptr<EType> NodeCache<EType>::get_copy(
//		const NodeCacheKey& key) const {
//	return const_cast<EType*>(get(key).get())->clone();
//}
//
//template<typename EType>
//const NodeCacheRef NodeCache<EType>::get_entry_metadata(
//		const NodeCacheKey& key) const {
//	auto res = this->get_int(key.semantic_id, key.entry_id);
//	return NodeCacheRef(type, key, *res);
//}

template<typename EType>
CacheStats NodeCache<EType>::get_stats() const {
	std::lock_guard<std::mutex> g(access_mtx);
	CacheStats result(type);
	for (auto &kv : access_tracker) {
		for (auto &id : kv.second) {
			try {
				auto e = this->get_int(kv.first, id);
				result.add_stats(kv.first,
						NodeEntryStats(id, e->last_access, e->access_count));
			} catch (const NoSuchElementException &nse2) {
				// Nothing to do... entry gone due to reorg
			}
		}
	}
	access_tracker.clear();
	return result;
}

template<typename EType>
void NodeCache<EType>::track_access(const NodeCacheKey& key,
		NodeCacheEntry<EType> &e) const {
	std::lock_guard<std::mutex> g(access_mtx);
	e.access_count++;
	e.last_access = CacheCommon::time_millis();
	try {
		access_tracker.at(key.semantic_id).insert(key.entry_id);
	} catch (const std::out_of_range &oor) {
		std::set<uint64_t> ids;
		ids.insert(key.entry_id);
		access_tracker.emplace(key.semantic_id, ids);
	}
}

// Instantiate all
template class NodeCache<GenericRaster> ;
template class NodeCache<PointCollection> ;
template class NodeCache<LineCollection> ;
template class NodeCache<PolygonCollection> ;
template class NodeCache<GenericPlot> ;
