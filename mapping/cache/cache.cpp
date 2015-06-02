/*
 * cache.cpp
 *
 *  Created on: 12.05.2015
 *      Author: mika
 */

#include <iostream>
#include <sstream>

#include "cache/cache.h"
#include "cache/common.h"
#include "raster/exceptions.h"
#include "operators/operator.h"


//
// List-Structure
//

//
// A simple std::vector based implementation of the
// STCacheStructure interface
//
template<typename EType>
class STListCacheStructure : public STCacheStructure<EType> {
public:
	virtual ~STListCacheStructure() {};
	virtual void insert( const std::shared_ptr<STCacheEntry<EType>> &entry );
	virtual std::shared_ptr<STCacheEntry<EType>> query( const QueryRectangle &spec );
	virtual void remove( const std::shared_ptr<STCacheEntry<EType>> &entry );
protected:
	virtual bool matches( const QueryRectangle &spec, const EType &ref ) const = 0;
private:
	std::vector<std::shared_ptr<STCacheEntry<EType>>> entries;
};

template<typename EType>
void STListCacheStructure<EType>::insert(
		const std::shared_ptr<STCacheEntry<EType>> &entry) {
	entries.push_back(entry);
}

template<typename EType>
std::shared_ptr<STCacheEntry<EType>> STListCacheStructure<EType>::query(
		const QueryRectangle &spec) {
	for ( auto entry : entries ) {
		if (matches( spec, *entry->result ) ) {
			return entry;
		}
	}
	return nullptr;
}

template<typename EType>
void STListCacheStructure<EType>::remove(
		const std::shared_ptr<STCacheEntry<EType>> &entry) {
	for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
		if (entry == *iter) {
			entries.erase(iter);
			return;
		}
	}
}

//
// List implementation of the raster cache
//

class STRasterCacheStructure : public STListCacheStructure<GenericRaster> {
public:
	virtual ~STRasterCacheStructure() {};
protected:
	virtual bool matches( const QueryRectangle &spec, const GenericRaster &entry ) const;
};

//
// Raster Structure
//

bool STRasterCacheStructure::matches(const QueryRectangle& spec,
		const GenericRaster &result) const {

	if (result.stref.timetype != TIMETYPE_UNIX)
		throw ArgumentException(
				std::string("Cache only accepts unix timestamps"));

	// Enlarge result by degrees of half a pixel in each direction
	double h_spacing = (result.stref.x2 - result.stref.x1) / result.width / 100.0;
	double v_spacing = (result.stref.y2 - result.stref.y1) / result.height / 100.0;

// DEBUGGING PART
	bool res = spec.epsg == result.stref.epsg;
	res &= spec.x1 >= result.stref.x1 - h_spacing;
	res &= spec.x2 <= result.stref.x2 + h_spacing;
	res &= spec.y1 >= result.stref.y1 - v_spacing;
	res &= spec.y2 <= result.stref.y2 + v_spacing;
	res &= spec.timestamp >= result.stref.t1;
	// FIXME: Shouldn't that be half open intervals? World1 -> Webmercator will never result in a hit if < instead of <= is used
	res &= spec.timestamp <= result.stref.t2;

	// Check query rectangle
	if ( spec.epsg == result.stref.epsg
			&& spec.x1 >= result.stref.x1 - h_spacing
			&& spec.x2 <= result.stref.x2 + h_spacing
			&& spec.y1 >= result.stref.y1 - v_spacing
			&& spec.y2 <= result.stref.y2 + v_spacing
			&& spec.timestamp >= result.stref.t1
			&& spec.timestamp <= result.stref.t2 ) {
		// Check resolution
		double ohspan = result.stref.x2 - result.stref.x1;
		double ovspan = result.stref.y2 - result.stref.y1;
		double qhspan = spec.x2 - spec.x1;
		double qvspan = spec.y2 - spec.y1;

		double hfact = qhspan / ohspan;
		double vfact = qvspan / ovspan;

		double clip_width  = result.width  * hfact;
		double clip_height = result.height * vfact;

		return clip_width >= spec.xres && clip_height >= spec.yres &&
			   clip_width < 2*spec.xres && clip_height < 2*spec.yres;
	}
	else
		return false;
}

//
// Cache implementation
//
template<typename EType>
STCache<EType>::~STCache() {
	for( auto &entry : caches ) {
		delete entry.second;
	}
}

template<typename EType>
STCacheStructure<EType>* STCache<EType>::get_structure(const std::string &key,
		bool create) {

	STCacheStructure<EType> *cache;
	auto got = caches.find(key);

	if (got == caches.end() && create) {
		Log::debug("No cache-structure for key found. Creating.");
		cache = new_structure();
		caches[key] = cache;
	} else if (got != caches.end())
		cache = got->second;
	else
		cache = nullptr;
	return cache;
}

template<typename EType>
void STCache<EType>::put(const std::string &key,
		const std::unique_ptr<EType> &item) {
	std::lock_guard<std::mutex> lock(mtx);
	Log::debug( "Adding entry for key \"%s\"", key.c_str());

	auto cache = get_structure(key, true);

	auto ce = new_entry(cache, item);
	Log::debug("Size of new Entry: %d bytes", ce->size);
	if (ce->size > max_size) {
		Log::warn(
				"Size of entry is greater than assigned cache-size of: %d bytes. Not inserting.",
				max_size);
	} else {
		if (current_size + ce->size > max_size) {
			Log::debug("New entry exhausts cache size. Cleaning up.");
			while (current_size + ce->size > max_size) {
				auto victim = policy->evict();
				Log::info("Evicting entry (%ld bytes): \"%s\"", victim->size, Common::stref_to_string(victim->result->stref).c_str() );
				victim->structure->remove(victim);
				current_size -= victim->size;
			};
			Log::debug("Cleanup finished. Free space: %d bytes",
					(max_size - current_size));
		}
		Log::debug("Inserting new entry into Cache-Structure.");
		current_size += ce->size;
		policy->inserted(ce);
		cache->insert(ce);
	}
}

template<typename EType>
std::unique_ptr<EType> STCache<EType>::get(const std::string &key,
		const QueryRectangle &qr) {
	std::lock_guard<std::mutex> lock(mtx);
	Log::debug("Get: Quering \"%s\" in cache \"%s\"",
				Common::qr_to_string(qr).c_str(), key.c_str());

	auto cache = get_structure(key);
	if (cache != nullptr) {
		auto entry = cache->query(qr);
		if (entry != nullptr) {
			policy->accessed(entry);
			Log::info("HIT for query \"%s\"", Common::qr_to_string(qr).c_str());
			return copy_content(entry->result);
		} else {
			Log::info("MISS for query \"%s\"", Common::qr_to_string(qr).c_str());
			throw NoSuchElementException("Entry not found");
		}
	} else {
		Log::info("MISS for query \"%s\"", Common::qr_to_string(qr).c_str());
		throw NoSuchElementException("Entry not found");
	}

}

template<typename EType>
std::shared_ptr<STCacheEntry<EType>> STCache<EType>::new_entry(
		STCacheStructure<EType>* structure,
		const std::unique_ptr<EType>& result) {
	return std::shared_ptr<STCacheEntry<EType>>(
			new STCacheEntry<EType>(copy_content(result), get_content_size(result),
					structure));
}

//
// RasterCache
//

size_t RasterCache::get_content_size(
		const std::unique_ptr<GenericRaster>& content) {
	// TODO: Get correct size
	return sizeof(*content) + content->getDataSize();
}

std::unique_ptr<GenericRaster> RasterCache::copy_content(
		const std::unique_ptr<GenericRaster> &content) {
	std::unique_ptr<GenericRaster> copy = GenericRaster::create(content->dd,
			*content.get(), content->getRepresentation());
	copy->blit(content.get(), 0);
	return copy;
}

STCacheStructure<GenericRaster>* RasterCache::new_structure() {
	return new STRasterCacheStructure();
}

// Instantiate all
template class STCache<GenericRaster> ;

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

//
// Default local cache
//
std::unique_ptr<GenericRaster> LocalCacheManager::get_raster(
		const std::string &semantic_id, const QueryRectangle &rect) {
	return rasterCache.get(semantic_id, rect);
}

void LocalCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	rasterCache.put( semantic_id, raster );
}

//
// NopCache
//
std::unique_ptr<GenericRaster> NopCacheManager::get_raster(
		const std::string& semantic_id, const QueryRectangle& rect) {
	(void) semantic_id;
	(void) rect;
	throw NoSuchElementException("Cache Miss");
}

void NopCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	(void) semantic_id;
	(void) raster;
	// Nothing TODO
}


std::unique_ptr<GenericRaster> RemoteCacheManager::get_raster(const std::string& semantic_id,
		const QueryRectangle& rect) {
	if ( remote_connection == nullptr )
		throw NetworkException("No connection to remote-index set.");

	// Send request
	uint8_t cmd = Common::CMD_INDEX_QUERY_CACHE;
	CacheRequest cr(rect,semantic_id);
	remote_connection->stream->write(cmd);
	cr.toStream(*remote_connection->stream);

	uint8_t resp;
	remote_connection->stream->read(&resp);
	switch ( resp ) {
		case Common::RESP_INDEX_HIT: {
			Log::debug("Index found cache-entry. Reading response.");
			DeliveryResponse dr(*remote_connection->stream);
			try {
				return Common::fetch_raster(dr);
			} catch ( std::exception &e ) {
				Log::error("Error while fetching raster from %s:%d", dr.host.c_str(), dr.port);
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

void RemoteCacheManager::put_raster(const std::string& semantic_id,
		const std::unique_ptr<GenericRaster>& raster) {
	//SocketConnection *con = thread_to_con.at(std::this_thread::get_id());
	(void) semantic_id;
	(void) raster;
	// TODO: Implement
}

std::unique_ptr<GenericRaster> HybridCacheManager::get_raster(const std::string& semantic_id,
		const QueryRectangle& rect) {
	try {
		return local_cache.get_raster(semantic_id,rect);
	} catch ( NoSuchElementException &nse ) {
		Log::debug("MISS on local cache. Asking index-server.");
		return RemoteCacheManager::get_raster(semantic_id, rect);
	}

}

void HybridCacheManager::put_raster(const std::string& semantic_id, const std::unique_ptr<GenericRaster>& raster) {
	try {
		RemoteCacheManager::put_raster(semantic_id,raster);
		local_cache.put_raster(semantic_id,raster);
	} catch ( std::exception &e ) {
		Log::error("Could not store entry in cache: %s", e.what());
	}
}
