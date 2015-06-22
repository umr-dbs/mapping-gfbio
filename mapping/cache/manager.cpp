/*
 * cache_manager.cpp
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#include "cache/manager.h"
#include "cache/common.h"
#include "node/puzzletracer.h"


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

CacheManager::~CacheManager() {
}

std::unique_ptr<GenericRaster> CacheManager::do_puzzle(const QueryRectangle &query,
	const geos::geom::Geometry &covered,
	const std::vector<std::unique_ptr<GenericRaster> >& items) {

	Log::trace("Puzzling raster with %d pieces", items.size() );

//	RasterWriter w = PuzzleTracer::get_writer();
//	w.write_meta(query,covered);

	double t1 = DoubleNegInfinity, t2 = DoubleInfinity;

	double qx1 = query.x1,
		   qy1 = query.y1,
		   qx2 = query.x2,
		   qy2 = query.y2;

	// Enlarge result rectangle as much as possible
	Log::trace("Maximizing result dimension");
	for ( auto &i : items ) {
		auto & r = i->stref;
		if ( r.x1 < qx1 && covered.contains( Common::create_square(r.x1,qy1,qx2,qy2).get() ) )
			qx1 = r.x1;

		if ( r.x2 > qx2 && covered.contains( Common::create_square(qx1,qy1,r.x2,qy2).get() ) )
					qx2 = r.x2;

		if ( r.y1 < qy1 && covered.contains( Common::create_square(qx1,r.y1,qx2,qy2).get() ) )
					qy1 = r.y1;

		if ( r.y2 > qy2 && covered.contains( Common::create_square(qx1,qy1,qx2,r.y2).get() ) )
					qy2 = r.y2;

		t1 = std::max(t1,r.t1);
		t2 = std::min(t2,r.t2);

//		w.write_raster(*i, "src_");
	}

	// Bake result
	Log::trace("Creating result raster");
	SpatialReference sref(query.epsg,qx1,qy1,qx2,qy2);
	TemporalReference tref(TIMETYPE_UNIX, t1, t2 );
	SpatioTemporalReference stref(sref,tref);

	auto &tmp = items.at(0);

	uint32_t width = std::floor((stref.x2-stref.x1) / tmp->pixel_scale_x);
	uint32_t height = std::floor((stref.y2-stref.y1) / tmp->pixel_scale_y);

	std::unique_ptr<GenericRaster> result = GenericRaster::create(
			tmp->dd,
			stref,
			width,
			height );

	for ( auto &raster : items ) {
		auto x = result->WorldToPixelX( raster->stref.x1 );
		auto y = result->WorldToPixelY( raster->stref.y1 );

		// TODO: Fix this case... Update: Should be fixed
		if ( x >= width || y >= height ||
			 x + raster->width <= 0 || y + raster->height <= 0 )
			Log::info("Puzzle piece out of result-raster, result: %s, piece: %s", Common::stref_to_string(result->stref).c_str(), Common::stref_to_string(raster->stref).c_str() );
		else {
			try {
				result->blit( raster.get(), x, y );
			} catch ( MetadataException &me ) {
				Log::error("Blit error. Result: %s, piece: %s", Common::stref_to_string(result->stref).c_str(), Common::stref_to_string(raster->stref).c_str() );
			}
		}
//		w.write_raster(*result, "dest_");
	}
	return result;
}


std::unique_ptr<GenericRaster> CacheManager::get_raster(const STCacheKey& key) {
	return get_raster(key.semantic_id, key.entry_id);
}

//
// Default local cache
//

LocalCacheManager::LocalCacheManager(size_t rasterCacheSize) : rasterCache(rasterCacheSize) {
}

LocalCacheManager::~LocalCacheManager() {
}

std::unique_ptr<GenericRaster> LocalCacheManager::query_raster(const GenericOperator &op,
	const QueryRectangle &rect) {

	typedef std::unique_ptr<GenericRaster> RP;
	typedef std::unique_ptr<geos::geom::Geometry> GP;

	Log::debug("Querying raster: %s on %s", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	STQueryResult res = rasterCache.query(op.getSemanticId(), rect);

	Log::debug("QueryResult: %s", res.to_string().c_str() );

	// Full single hit
	if ( !res.has_remainder() && res.ids.size() == 1 ) {
		Log::debug("Full single HIT for query: %s on %s. Returning cached raster.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		return rasterCache.get_copy(op.getSemanticId(), res.ids.at(0) );
	}
	else if ( res.has_hit() ) {
		Log::debug("Full HIT for query: %s on %s. Puzzling result.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		std::vector<CacheRef> refs;
		for ( auto &id : res.ids )
			refs.push_back( CacheRef("fakehost",1,id) );

		PuzzleRequest rpr( op.getSemanticId(), rect, res.covered, res.remainder, refs );
		RP result = Common::process_raster_puzzle(rpr,"fakehost",1);
		put_raster( op.getSemanticId(), result );
		return result;
	}
	// Miss
	else {
		Log::debug("Full MISS for query: %s on %s", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		throw NoSuchElementException("Cache Miss");
	}
}

std::unique_ptr<GenericRaster> LocalCacheManager::get_raster(const std::string& semantic_id,
	uint64_t entry_id) {
	Log::debug("Retrieving raster-cache-entry: %s::%d", semantic_id.c_str(), entry_id);
	return rasterCache.get_copy(semantic_id, entry_id);
}

void LocalCacheManager::put_raster(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	Log::debug("Adding raster to cache: x: [%f,%f], y[%f,%f], t[%f,%f], size: %dx%d, res: %fx%f",
		raster->stref.x1,raster->stref.x2,raster->stref.y1,raster->stref.y2,
		raster->stref.t1,raster->stref.t2, raster->width, raster->height,
		raster->pixel_scale_x,raster->pixel_scale_y);
	rasterCache.put(semantic_id, raster);
}

//
// NopCache
//

NopCacheManager::~NopCacheManager() {
}

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
	// Nothing to-do
}

//
// Remote Cache
//

RemoteCacheManager::RemoteCacheManager(size_t rasterCacheSize, const std::string &my_host, uint32_t my_port) :
	local_cache(rasterCacheSize), my_host(my_host), my_port(my_port) {
}

RemoteCacheManager::~RemoteCacheManager() {
}

std::unique_ptr<GenericRaster> RemoteCacheManager::query_raster(const GenericOperator &op,
	const QueryRectangle& rect) {

	Log::debug("Querying raster: %s on %s", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	// Local lookup
	STQueryResult qres = local_cache.query(op.getSemanticId(), rect);

	Log::debug("QueryResult: %s", qres.to_string().c_str() );

	// Full single local hit
	if ( !qres.has_remainder() && qres.ids.size() == 1 ) {
		Log::debug("Full single local HIT for query: %s on %s. Returning cached raster.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		return local_cache.get_copy(op.getSemanticId(), qres.ids.at(0) );
	}
	// Full local hit (puzzle)
	else if ( !qres.has_remainder() ) {
		Log::debug("Full local HIT for query: %s on %s. Puzzling result.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		std::vector<CacheRef> refs;
		for ( auto &id : qres.ids )
			refs.push_back( CacheRef(my_host,my_port,id) );

		PuzzleRequest rpr( op.getSemanticId(), rect, qres.covered, qres.remainder, refs );
		std::unique_ptr<GenericRaster> result = Common::process_raster_puzzle(rpr,my_host,my_port);
		put_raster( op.getSemanticId(), result );
		return result;
	}
	// Check index (if we aren't on depth = 0 )
	else if ( op.getDepth() != 0 ) {
		Log::debug("Local MISS for query: %s on %s. Querying index.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		uint8_t cmd = Common::CMD_INDEX_QUERY_RASTER_CACHE;
		BaseRequest cr(op.getSemanticId(), rect);
		remote_connection->stream->write(cmd);
		cr.toStream(*remote_connection->stream);

		uint8_t resp;
		remote_connection->stream->read(&resp);
		switch (resp) {
			// Full hit on different client
			case Common::RESP_INDEX_HIT: {
				Log::debug("Full single remote HIT for query: %s on %s. Returning cached raster.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
				CacheRef cr(*remote_connection->stream);
				return Common::fetch_raster(cr.host,cr.port,STCacheKey(op.getSemanticId(),cr.entry_id));
				break;
			}
			// Full miss on whole cache
			case Common::RESP_INDEX_MISS: {
				Log::debug("Full remote MISS for query: %s on %s.", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
				throw NoSuchElementException("Cache-Miss.");
				break;
			}
			// Puzzle time
			case Common::RESP_INDEX_PARTIAL: {
				PuzzleRequest pr(*remote_connection->stream);
				Log::debug("Partial remote HIT for query: %s on %s: %s", Common::qr_to_string(rect).c_str(), op.getSemanticId().c_str(), pr.to_string().c_str() );
				auto res = Common::process_raster_puzzle(pr,my_host,my_port);
				put_raster(op.getSemanticId(),res);
				return res;
				break;
			}
			default: {
				throw NetworkException("Received unknown response from index.");
			}
		}
	}
	else {
		Log::debug("Graph-Depth = 0, omitting index query, returning MISS.");
		throw NoSuchElementException("Cache-Miss");
	}
}

std::unique_ptr<GenericRaster> RemoteCacheManager::get_raster(const std::string& semantic_id,
	uint64_t entry_id) {
	Log::debug("Getting raster from local cache. ID: %s::%d", semantic_id.c_str(), entry_id);
	return local_cache.get_copy(semantic_id, entry_id);
}

void RemoteCacheManager::put_raster(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	if (remote_connection == nullptr)
		throw NetworkException("No connection to remote-index.");

	Log::debug("Adding raster to cache: x: [%f,%f], y[%f,%f], t[%f,%f], size: %dx%d, res: %fx%f",
			raster->stref.x1,raster->stref.x2,raster->stref.y1,raster->stref.y2,
			raster->stref.t1,raster->stref.t2, raster->width, raster->height,
			raster->pixel_scale_x,raster->pixel_scale_y);

	Log::debug("Adding raster to local cache.");
	auto id = local_cache.put(semantic_id, raster);
	STRasterEntryBounds cube(*raster);

	Log::debug("Adding raster to remote cache.");
	uint8_t cmd = Common::RESP_WORKER_NEW_RASTER_CACHE_ENTRY;
	remote_connection->stream->write(cmd);
	id.toStream(*remote_connection->stream);
	cube.toStream(*remote_connection->stream);
	Log::debug("Finished adding raster to cache.");
	// TODO: Do we need a confirmation?
}
