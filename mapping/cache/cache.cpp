/*
 * cache.cpp
 *
 *  Created on: 12.05.2015
 *      Author: mika
 */

#include "cache/cache.h"
#include "cache/common.h"
#include "raster/exceptions.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <iostream>
#include <sstream>
#include <unordered_map>

STCacheKey::STCacheKey(const std::string& semantic_id, uint64_t entry_id) :
		semantic_id(semantic_id), entry_id(entry_id) {
}

STCacheKey::STCacheKey(BinaryStream& stream) {
	stream.read(&semantic_id);
	stream.read(&entry_id);
}

void STCacheKey::toStream(BinaryStream& stream) const {
	stream.write(semantic_id);
	stream.write(entry_id);
}



//
// Represents a single entry in the cache
//
template<typename EType>
class STCacheEntry {
public:
	STCacheEntry(uint64_t id, std::shared_ptr<EType> result, std::unique_ptr<CacheCube> cube, uint64_t size) :
			id(id), result(result), cube(std::move(cube)), size(sizeof(STCacheEntry) + size) {
	}
	;
	~STCacheEntry() {
		Log::debug("STCacheEntry destroyed");
	}
	const uint64_t id;
	const std::shared_ptr<EType> result;
	const std::unique_ptr<CacheCube> cube;
	const uint64_t size;
};

//
// A simple std::map based implementation of the
// STCacheStructure interface
//
template<typename EType>
class STMapCacheStructure: public STCacheStructure<EType> {
public:
	STMapCacheStructure() : next_id(1) {}
	;
	virtual ~STMapCacheStructure() {
	}
	;
	virtual uint64_t insert(const std::unique_ptr<EType> &result);
	virtual std::unique_ptr<EType> get_copy(const uint64_t id) const;
	virtual std::unique_ptr<EType> query_copy(const QueryRectangle &spec) const;
	virtual const std::shared_ptr<EType> get(const uint64_t id) const;
	virtual const std::shared_ptr<EType> query(const QueryRectangle &spec) const;
	virtual uint64_t get_entry_size(const uint64_t id) const;
	virtual void remove(const uint64_t id);
protected:
	virtual std::unique_ptr<EType> copy(const EType &content) const = 0;
	virtual std::unique_ptr<CacheCube> create_cube(const EType &content) const = 0;
	virtual uint64_t get_content_size(const EType &content) const = 0;
private:
	std::unordered_map<uint64_t, std::shared_ptr<STCacheEntry<EType>>>entries;
	uint64_t next_id;
};

template<typename EType>
uint64_t STMapCacheStructure<EType>::insert(const std::unique_ptr<EType> &result) {
	auto cpy = copy(*result);
	std::shared_ptr<EType> sp(cpy.release());

	std::shared_ptr<STCacheEntry<EType>> entry(
			new STCacheEntry<EType>( next_id++,
									 sp,
									 create_cube(*result),
									 get_content_size(*result) ) );

	entries[entry->id] = entry;
	return entry->id;
}

template<typename EType>
const std::shared_ptr<EType> STMapCacheStructure<EType>::get(const uint64_t id) const {
	try {
		auto &e = entries.at(id);
		return e->result;
	} catch (std::out_of_range &oor) {
		std::ostringstream msg;
		msg << "No cache-entry with id: " << id;
		throw NoSuchElementException(msg.str());
	}
}

template<typename EType>
std::unique_ptr<EType> STMapCacheStructure<EType>::get_copy(const uint64_t id) const {
	auto tmp = get(id);
	return copy(*tmp);
}

template<typename EType>
const std::shared_ptr<EType> STMapCacheStructure<EType>::query(const QueryRectangle& spec) const {
	for (auto &e : entries) {
		if (e.second->cube->matches(spec))
			return e.second->result;
	}
	std::ostringstream msg;
	msg << "No entry matching query: " << Common::qr_to_string(spec);
	throw NoSuchElementException(msg.str());
}

template<typename EType>
std::unique_ptr<EType> STMapCacheStructure<EType>::query_copy(const QueryRectangle& spec) const {
	auto tmp = query(spec);
	return copy(*tmp);
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
	entries.erase(id);
}

//
// Map implementation of the raster cache
//

class STRasterCacheStructure: public STMapCacheStructure<GenericRaster> {
public:
	virtual ~STRasterCacheStructure() {
	}
	;
protected:
	virtual std::unique_ptr<GenericRaster> copy(const GenericRaster &content) const;
		virtual std::unique_ptr<CacheCube> create_cube(const GenericRaster &content) const;
		virtual uint64_t get_content_size(const GenericRaster &content) const;
};

std::unique_ptr<GenericRaster> STRasterCacheStructure::copy(
		const GenericRaster& content) const {
	auto copy = GenericRaster::create(content.dd, content, content.getRepresentation());
	copy->blit(&content, 0);
	return copy;
}

std::unique_ptr<CacheCube> STRasterCacheStructure::create_cube(
		const GenericRaster& content) const {
	return std::make_unique<RasterCacheCube>(content);
}

uint64_t STRasterCacheStructure::get_content_size(const GenericRaster& content) const {
	// TODO: Get correct size
	return sizeof(content) + content.getDataSize();
}


//
// Index cache
//

class RasterRefStructure : public STCacheStructure<RasterRef> {
public:
	RasterRefStructure() : next_id(1) {};
	virtual ~RasterRefStructure() {};
	virtual uint64_t insert(const std::unique_ptr<RasterRef> &result);
	virtual std::unique_ptr<RasterRef> get_copy(const uint64_t id) const;
	virtual std::unique_ptr<RasterRef> query_copy(const QueryRectangle &spec) const;
	virtual const std::shared_ptr<RasterRef> get(const uint64_t id) const;
	virtual const std::shared_ptr<RasterRef> query(const QueryRectangle &spec) const;
	virtual uint64_t get_entry_size(const uint64_t id) const;
	virtual void remove(const uint64_t id);
private:
	std::unordered_map<uint64_t, std::shared_ptr<RasterRef>> entries;
	uint64_t next_id;
};

uint64_t RasterRefStructure::insert(const std::unique_ptr<RasterRef>& result) {
	uint64_t id = next_id++;
	entries.emplace(id,std::shared_ptr<RasterRef>( new RasterRef(*result) ) );
	return id;
}

const std::shared_ptr<RasterRef> RasterRefStructure::get(const uint64_t id) const {
	try {
		return entries.at(id);
	} catch ( std::out_of_range &oor ) {
		throw NoSuchElementException("No entry found");
	}
}

std::unique_ptr<RasterRef> RasterRefStructure::get_copy(const uint64_t id) const {
	return std::make_unique<RasterRef>( *get(id) );
}

const std::shared_ptr<RasterRef> RasterRefStructure::query(const QueryRectangle& spec) const {
	for ( auto &e : entries ) {
		if ( e.second->cube.matches(spec) )
			return e.second;
	}
	throw NoSuchElementException("No entry found");
}

std::unique_ptr<RasterRef> RasterRefStructure::query_copy(const QueryRectangle& spec) const {
	return std::make_unique<RasterRef>( *query(spec) );
}

uint64_t RasterRefStructure::get_entry_size(const uint64_t id) const {
	(void) id;
	return 0;
}

void RasterRefStructure::remove(const uint64_t id) {
	entries.erase(id);
}




//
// Cache implementation
//
template<typename EType>
STCache<EType>::~STCache() {
	for (auto &entry : caches) {
		delete entry.second;
	}
}

template<typename EType>
STCacheStructure<EType>* STCache<EType>::get_structure(const std::string &key, bool create) const {

	STCacheStructure<EType> *cache;
	auto got = caches.find(key);

	if (got == caches.end() && create) {
		Log::debug("No cache-structure for key found. Creating.");
		cache = new_structure();
		caches[key] = cache;
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
	if (cache != nullptr)
		return cache->remove(id);
}

template<typename EType>
STCacheKey STCache<EType>::put(const std::string &key, const std::unique_ptr<EType> &item) {
	std::lock_guard<std::mutex> lock(mtx);
	Log::debug("Adding entry for key \"%s\"", key.c_str());

	auto cache = get_structure(key, true);
	auto id = cache->insert(item);
	auto size = cache->get_entry_size(id);
	current_size += size;

	Log::debug("Size of new Entry: %d bytes", size);
//	if (size > max_size) {
//		Log::warn("Size of entry is greater than assigned cache-size of: %d bytes. Not inserting.", max_size);
//	}
//	else {
//		if (current_size + ce->size > max_size) {
//			Log::debug("New entry exhausts cache size. Cleaning up.");
//			while (current_size + ce->size > max_size) {
//				auto victim = policy->evict();
//				Log::info("Evicting entry (%ld bytes): \"%s\"", victim->size,
//						Common::stref_to_string(victim->result->stref).c_str());
//				victim->structure->remove(victim);
//				current_size -= victim->size;
//			};
//			Log::debug("Cleanup finished. Free space: %d bytes", (max_size - current_size));
//		}
//	}
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
const std::shared_ptr<EType> STCache<EType>::query(const std::string &key, const QueryRectangle &qr) const {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(key);
	if (cache != nullptr)
		return cache->query(qr);
	else {
		std::ostringstream msg;
		throw NoSuchElementException("Entry not found");
	}

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
std::unique_ptr<EType> STCache<EType>::query_copy(const std::string &key, const QueryRectangle &qr) const {
	std::lock_guard<std::mutex> lock(mtx);
	auto cache = get_structure(key);
	if (cache != nullptr)
		return cache->query_copy(qr);
	else {
		std::ostringstream msg;
		throw NoSuchElementException("Entry not found");
	}
}

//
// RasterCache
//

STCacheStructure<GenericRaster>* RasterCache::new_structure() const {
	return new STRasterCacheStructure();
}


//
// Index Cache
//
STCacheStructure<RasterRef>* RasterRefCache::new_structure() const {
	return new RasterRefStructure();
}

//
// We have to manage our *_by_node structure here
//

STCacheKey RasterRefCache::put(const std::string& semantic_id, const std::unique_ptr<RasterRef>& item) {
	STCacheKey key = STCache<RasterRef>::put(semantic_id,item);
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
		STCache<RasterRef>::remove(semantic_id,id);
	} catch ( NoSuchElementException &nse ) {
		// Nothing todo here
	}
}

void RasterRefCache::remove_all_by_node(uint32_t node_id) {
	try {
		for ( auto &k : entries_by_node.at(node_id) )
			STCache<RasterRef>::remove( k );
	} catch ( std::out_of_range &oor ) {

	}
	entries_by_node.erase(node_id);
}

// Instantiate all
template class STCache<GenericRaster> ;
template class STCache<RasterRef>;


//
// Cache-Manager
//
std::unique_ptr<CacheManager> CacheManager::impl;

CacheManager& CacheManager::getInstance() {
	if (CacheManager::impl)
		return *impl;
	else
		throw NotInitializedException(
				"CacheManager was not initialized. Please use CacheManager::init first.");
}

void CacheManager::init(std::unique_ptr<CacheManager>& impl) {
	CacheManager::impl.reset(impl.release());
}

thread_local SocketConnection* CacheManager::remote_connection = nullptr;

std::unique_ptr<GenericRaster> CacheManager::get_raster(const STCacheKey& key) {
	return get_raster(key.semantic_id,key.entry_id);
}

//
// Default local cache
//
std::unique_ptr<GenericRaster> LocalCacheManager::query_raster(const GenericOperator &op,
		const QueryRectangle &rect) {
	return rasterCache.query_copy(op.getSemanticId(), rect);
}

std::unique_ptr<GenericRaster> LocalCacheManager::get_raster(const std::string& semantic_id,
		uint64_t entry_id) {
	return rasterCache.get_copy(semantic_id,entry_id);
}

void LocalCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	rasterCache.put(semantic_id, raster);
}

//
// NopCache
//
std::unique_ptr<GenericRaster> NopCacheManager::query_raster(const GenericOperator &op,
		const QueryRectangle& rect) {
	(void) op;
	(void) rect;
	throw NoSuchElementException("Cache Miss");
}

std::unique_ptr<GenericRaster> NopCacheManager::get_raster(const std::string& semantic_id,
		uint64_t entry_id) {
	(void) semantic_id;
	(void) entry_id;
	throw NoSuchElementException("Cache Miss");
}

void NopCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	(void) semantic_id;
	(void) raster;
	// Nothing TODO
}

//
// Remote Cache
//

void RemoteCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	if (remote_connection == nullptr)
		throw NetworkException("No connection to remote-index set.");

	auto id = local_cache.put(semantic_id, raster);
	RasterCacheCube cube(*raster);

	uint8_t cmd = Common::RESP_WORKER_NEW_RASTER_CACHE_ENTRY;
	remote_connection->stream->write(cmd);
	id.toStream(*remote_connection->stream);
	cube.toStream(*remote_connection->stream);
	// TODO: Do we need a confirmation
}

std::unique_ptr<GenericRaster> RemoteCacheManager::query_raster(const GenericOperator &op,
		const QueryRectangle& rect) {
	try {
		std::unique_ptr<GenericRaster> res;
		// Omit remote lookup on root operator
		if (op.getDepth() == 0) {
			res = local_cache.query_copy(op.getSemanticId(), rect);
			Log::info("HIT on local cache.");
		}
		else {
			try {
				res = local_cache.query_copy(op.getSemanticId(), rect);
				Log::info("HIT on local cache.");
			} catch (NoSuchElementException &nse) {

				Log::info("MISS on local cache. Asking index-server.");
				res = get_raster_from_remote(op, rect);
				Log::info("HIT on remote cache.");
			}
		}
		return res;
	} catch ( NoSuchElementException &nse2 ) {
		Log::info("MISS on local and remote cache.");
		throw nse2;
	}
}

std::unique_ptr<GenericRaster> RemoteCacheManager::get_raster(const std::string& semantic_id,
		uint64_t entry_id) {
	return local_cache.get_copy(semantic_id,entry_id);
}

std::unique_ptr<GenericRaster> RemoteCacheManager::get_raster_from_remote(const GenericOperator &op,
		const QueryRectangle& rect) {
	if (remote_connection == nullptr)
		throw NetworkException("No connection to remote-index set.");

	// Send request
	uint8_t cmd = Common::CMD_INDEX_QUERY_RASTER_CACHE;
	CacheRequest cr(rect, op.getSemanticId());
	remote_connection->stream->write(cmd);
	cr.toStream(*remote_connection->stream);

	uint8_t resp;
	remote_connection->stream->read(&resp);
	switch (resp) {
		case Common::RESP_INDEX_HIT: {
			Log::debug("Index found cache-entry. Reading response.");
			std::string host;
			uint32_t port;
			uint64_t id;
			remote_connection->stream->read(&host);
			remote_connection->stream->read(&port);
			remote_connection->stream->read(&id);
			try {
				return Common::fetch_raster(host, port, STCacheKey(op.getSemanticId(),id) );
			} catch (std::exception &e) {
				Log::error("Error while fetching raster from %s:%d", host.c_str(), port);
				// TODO: Maybe not...
				throw NoSuchElementException("Error fetching raster from remote");
			}
			break;
		}
		case Common::RESP_INDEX_MISS: {
			Log::debug("MISS from index-server.");
			throw NoSuchElementException("No cache-entry found on index.");
		}
		default: {
			throw NetworkException("Received unknown response from index.");
		}
	}
}
