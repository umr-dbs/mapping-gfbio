/*
 * cache_manager.cpp
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/manager.h"
#include "cache/priv/connection.h"
#include "node/puzzletracer.h"
#include "util/log.h"


//
// Cache-Manager
//
std::unique_ptr<CacheManager> CacheManager::impl;
std::unique_ptr<CachingStrategy> CacheManager::strategy;

CacheManager& CacheManager::getInstance() {
	if (CacheManager::impl)
		return *impl;
	else
		throw NotInitializedException(
			"CacheManager was not initialized. Please use CacheManager::init first.");
}

CachingStrategy& CacheManager::get_strategy() {
	if (CacheManager::strategy)
		return *strategy;
	else
		throw NotInitializedException(
			"CacheManager was not initialized. Please use CacheManager::init first.");
}

void CacheManager::init(std::unique_ptr<CacheManager> impl, std::unique_ptr<CachingStrategy> strategy) {
	CacheManager::impl.reset(impl.release());
	CacheManager::strategy.reset(strategy.release());
}

thread_local UnixSocket* CacheManager::remote_connection = nullptr;

CacheManager::~CacheManager() {
}

std::unique_ptr<GenericRaster> CacheManager::fetch_raster(const std::string & host, uint32_t port,
	const NodeCacheKey &key) {
	Log::debug("Fetching cache-entry from: %s:%d, key: %s", host.c_str(), port, key.to_string().c_str());
	UnixSocket sock(host.c_str(), port);

	BinaryStream &stream = sock;
	stream.write(DeliveryConnection::MAGIC_NUMBER);
	stream.write(DeliveryConnection::CMD_GET_CACHED_RASTER);
	key.toStream(stream);

	uint8_t resp;
	stream.read(&resp);
	switch (resp) {
		case DeliveryConnection::RESP_OK: {
			return GenericRaster::fromStream(stream);
		}
		case DeliveryConnection::RESP_ERROR: {
			std::string err_msg;
			stream.read(&err_msg);
			Log::error("Delivery returned error: %s", err_msg.c_str());
			throw DeliveryException(err_msg);
		}
		default: {
			Log::error("Delivery returned unknown code: %d", resp);
			throw DeliveryException("Delivery returned unknown code");
		}
	}
}

std::unique_ptr<GenericRaster> CacheManager::process_raster_puzzle(const PuzzleRequest& req, std::string my_host,
	uint32_t my_port) {
	typedef std::shared_ptr<GenericRaster> RP;
	typedef std::unique_ptr<geos::geom::Geometry> GP;

	Log::trace("Processing puzzle-request: %s", req.to_string().c_str());

	std::vector<RP> items;

	GP covered(req.covered->clone());

	// Fetch puzzle parts
	Log::trace("Fetching all puzzle-parts");
	for (const CacheRef &cr : req.parts) {
		if (cr.host == my_host && cr.port == my_port) {
			Log::trace("Fetching puzzle-piece from local cache, key: %s:%d", req.semantic_id.c_str(),
				cr.entry_id);
			items.push_back(CacheManager::getInstance().get_raster_ref( NodeCacheKey(req.semantic_id, cr.entry_id) ));
		}
		else {
			Log::debug("Fetching puzzle-piece from %s:%d, key: %s:%d", cr.host.c_str(), cr.port,
				req.semantic_id.c_str(), cr.entry_id);
			auto raster = fetch_raster(cr.host, cr.port, NodeCacheKey(req.semantic_id, cr.entry_id));
			items.push_back( std::shared_ptr<GenericRaster>(raster.release()) );
		}
	}

	// Create remainder
	if (!req.remainder->isEmpty()) {
		Log::trace("Creating remainder: %s", req.remainder->toString().c_str());
		auto graph = GenericOperator::fromJSON(req.semantic_id);
		QueryProfiler qp;
		auto &f = items.at(0);

		QueryRectangle rqr = req.get_remainder_query(f->pixel_scale_x, f->pixel_scale_y);

		try {
			auto rem = graph->getCachedRaster(rqr, qp, GenericOperator::RasterQM::LOOSE);

			if (std::abs(1.0 - f->pixel_scale_x / rem->pixel_scale_x) > 0.01
				|| std::abs(1.0 - f->pixel_scale_y / rem->pixel_scale_y) > 0.01) {
				Log::error(
					"Resolution clash on remainder. Requires: [%f,%f], result: [%f,%f], QueryRectangle: [%f,%f], %s",
					f->pixel_scale_x, f->pixel_scale_y, rem->pixel_scale_x, rem->pixel_scale_y,
					((rqr.x2 - rqr.x1) / rqr.xres), ((rqr.y2 - rqr.y1) / rqr.yres),
					CacheCommon::qr_to_string(rqr).c_str());

				throw OperatorException("Incompatible resolution on remainder");
			}

			CacheEntryBounds bounds(*rem);
			GP cube_square = CacheCommon::create_square(bounds.x1, bounds.y1, bounds.x2, bounds.y2);
			covered = GP(covered->Union(cube_square.get()));
			items.push_back( std::shared_ptr<GenericRaster>(rem.release()) );
		} catch ( const MetadataException &me) {
			Log::error("Error fetching remainder: %s. Query: %s", me.what(), CacheCommon::qr_to_string(rqr).c_str());
			throw;
		}
	}

	auto result = CacheManager::do_puzzle(req.query, *covered, items);
	Log::trace("Finished processing puzzle-request: %s", req.to_string().c_str());
	return result;
}

std::unique_ptr<GenericRaster> CacheManager::do_puzzle(const QueryRectangle &query,
	const geos::geom::Geometry &covered,
	const std::vector<std::shared_ptr<GenericRaster> >& items) {

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
		if ( r.x1 < qx1 && covered.contains( CacheCommon::create_square(r.x1,qy1,qx2,qy2).get() ) )
			qx1 = r.x1;

		if ( r.x2 > qx2 && covered.contains( CacheCommon::create_square(qx1,qy1,r.x2,qy2).get() ) )
					qx2 = r.x2;

		if ( r.y1 < qy1 && covered.contains( CacheCommon::create_square(qx1,r.y1,qx2,qy2).get() ) )
					qy1 = r.y1;

		if ( r.y2 > qy2 && covered.contains( CacheCommon::create_square(qx1,qy1,qx2,r.y2).get() ) )
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

		if ( x >= width || y >= height ||
			 x + raster->width <= 0 || y + raster->height <= 0 )
			Log::info("Puzzle piece out of result-raster, result: %s, piece: %s", CacheCommon::stref_to_string(result->stref).c_str(), CacheCommon::stref_to_string(raster->stref).c_str() );
		else {
			try {
				result->blit( raster.get(), x, y );
			} catch ( const MetadataException &me ) {
				Log::error("Blit error. Result: %s, piece: %s", CacheCommon::stref_to_string(result->stref).c_str(), CacheCommon::stref_to_string(raster->stref).c_str() );
			}
		}
//		w.write_raster(*result, "dest_");
	}
	return result;
}


//
// Default local cache
//

LocalCacheManager::LocalCacheManager(size_t rasterCacheSize) : raster_cache(rasterCacheSize) {
}

LocalCacheManager::~LocalCacheManager() {
}

std::unique_ptr<GenericRaster> LocalCacheManager::query_raster(const GenericOperator &op,
	const QueryRectangle &rect) {

	typedef std::unique_ptr<GenericRaster> RP;

	Log::debug("Querying raster: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	CacheQueryResult<uint64_t> res = raster_cache.query(op.getSemanticId(), rect);

	Log::debug("QueryResult: %s", res.to_string().c_str() );

	// Full single hit
	if ( !res.has_remainder() && res.keys.size() == 1 ) {
		NodeCacheKey key(op.getSemanticId(), res.keys.at(0));
		Log::trace("Full single HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		return raster_cache.get_copy(key);
	}
	else if ( res.has_hit() ) {
		Log::trace("Full HIT for query: %s on %s. Puzzling result.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		std::vector<CacheRef> refs;
		for ( auto &id : res.keys )
			refs.push_back( CacheRef("fakehost",1,id) );

		PuzzleRequest rpr( op.getSemanticId(), rect, res.covered, res.remainder, refs );
		RP result = process_raster_puzzle(rpr,"fakehost",1);
		put_raster( op.getSemanticId(), result );
		return result;
	}
	// Miss
	else {
		Log::trace("Full MISS for query: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		throw NoSuchElementException("Cache Miss");
	}
}


void LocalCacheManager::put_raster(const std::string& semantic_id, const std::unique_ptr<GenericRaster>& raster) {
	put_raster_local(semantic_id,raster);
}

const std::shared_ptr<GenericRaster> LocalCacheManager::get_raster_ref(const NodeCacheKey& key) {
	Log::debug("Getting raster from local cache. Key: %s", key.to_string().c_str());
	return raster_cache.get(key);
}

NodeCacheRef LocalCacheManager::put_raster_local(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	Log::debug("Adding raster to local cache: %s", CacheCommon::raster_to_string(*raster).c_str());
	return raster_cache.put(semantic_id, raster);
}

void LocalCacheManager::remove_raster_local(const NodeCacheKey &key) {
	Log::debug("Removing raster from local cache. Key: %s", key.to_string().c_str());
	raster_cache.remove(key);
}

NodeHandshake LocalCacheManager::get_handshake(const std::string& my_host, uint32_t my_port) const {
	Log::debug("Generating handshake infos.");
	return NodeHandshake(
		my_host,
		my_port,
		Capacity( raster_cache.get_max_size(), raster_cache.get_current_size() ),
		raster_cache.get_all()
	);
}

NodeStats LocalCacheManager::get_stats() const {
	Log::debug("Generating cache statistics.");
	return NodeStats(
		Capacity( raster_cache.get_max_size(), raster_cache.get_current_size() ),
		raster_cache.get_stats()
	);
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

void NopCacheManager::put_raster(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	(void) semantic_id;
	(void) raster;
	// Nothing to-do
}

const std::shared_ptr<GenericRaster> NopCacheManager::get_raster_ref(const NodeCacheKey& key) {
	(void) key;
	throw NoSuchElementException("Cache Miss");
}

NodeCacheRef NopCacheManager::put_raster_local(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	(void) semantic_id;
	(void) raster;
	CacheEntry ce(CacheEntryBounds(*raster), 0 );
	return NodeCacheRef(semantic_id,1,ce);
	// Nothing to-do
}

void NopCacheManager::remove_raster_local(const NodeCacheKey &key) {
	(void) key;
	// Nothing to-do
}

NodeHandshake NopCacheManager::get_handshake(const std::string& my_host, uint32_t my_port) const {
	return NodeHandshake(
		my_host,
		my_port,
		Capacity( 0,0 ),
		std::vector<NodeCacheRef>()
	);
}

NodeStats NopCacheManager::get_stats() const {
	return NodeStats(
		Capacity( 0,0 ),
		CacheStats()
	);
}

//
// Remote Cache
//

RemoteCacheManager::RemoteCacheManager(size_t raster_cache_size, const std::string &my_host, uint32_t my_port) :
	LocalCacheManager(raster_cache_size), my_host(my_host), my_port(my_port) {
}

RemoteCacheManager::~RemoteCacheManager() {
}

std::unique_ptr<GenericRaster> RemoteCacheManager::query_raster(const GenericOperator &op,
	const QueryRectangle& rect) {

	Log::debug("Querying raster: %s on %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str() );

	// Local lookup
	CacheQueryResult<uint64_t> qres = raster_cache.query(op.getSemanticId(), rect);

	Log::debug("QueryResult: %s", qres.to_string().c_str() );

	// Full single local hit
	if ( !qres.has_remainder() && qres.keys.size() == 1 ) {
		Log::trace("Full single local HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		NodeCacheKey key(op.getSemanticId(), qres.keys.at(0));
		return raster_cache.get_copy(key);
	}
	// Full local hit (puzzle)
	else if ( !qres.has_remainder() ) {
		Log::trace("Full local HIT for query: %s on %s. Puzzling result.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		std::vector<CacheRef> refs;
		for ( auto &id : qres.keys )
			refs.push_back( CacheRef(my_host,my_port,id) );

		PuzzleRequest rpr( op.getSemanticId(), rect, qres.covered, qres.remainder, refs );
		std::unique_ptr<GenericRaster> result = process_raster_puzzle(rpr,my_host,my_port);
		put_raster( op.getSemanticId(), result );
		return result;
	}
	// Check index (if we aren't on depth = 0 )
	else if ( op.getDepth() != 0 ) {
		Log::debug("Local MISS for query: %s on %s. Querying index.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
		BaseRequest cr(op.getSemanticId(), rect);
		BinaryStream &stream = *remote_connection;

		stream.write(WorkerConnection::CMD_QUERY_RASTER_CACHE);
		cr.toStream(stream);

		uint8_t resp;
		stream.read(&resp);
		switch (resp) {
			// Full hit on different client
			case WorkerConnection::RESP_QUERY_HIT: {
				Log::trace("Full single remote HIT for query: %s on %s. Returning cached raster.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
				CacheRef cr(stream);
				return fetch_raster(cr.host,cr.port,NodeCacheKey(op.getSemanticId(),cr.entry_id));
				break;
			}
			// Full miss on whole cache
			case WorkerConnection::RESP_QUERY_MISS: {
				Log::trace("Full remote MISS for query: %s on %s.", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str());
				throw NoSuchElementException("Cache-Miss.");
				break;
			}
			// Puzzle time
			case WorkerConnection::RESP_QUERY_PARTIAL: {
				PuzzleRequest pr(stream);
				Log::trace("Partial remote HIT for query: %s on %s: %s", CacheCommon::qr_to_string(rect).c_str(), op.getSemanticId().c_str(), pr.to_string().c_str() );
				auto res = process_raster_puzzle(pr,my_host,my_port);
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

void RemoteCacheManager::put_raster(const std::string& semantic_id,
	const std::unique_ptr<GenericRaster>& raster) {
	if (remote_connection == nullptr)
		throw NetworkException("No connection to remote-index.");

	auto ref = put_raster_local(semantic_id, raster);
	BinaryStream &stream = *remote_connection;

	Log::debug("Adding raster to remote cache: %s", ref.to_string().c_str());
	stream.write(WorkerConnection::RESP_NEW_RASTER_CACHE_ENTRY);
	ref.toStream(stream);
}
