/*
 * cache_manager.cpp
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#include "cache/manager.h"
#include "cache/priv/transfer.h"
#include "cache/priv/connection.h"
#include "util/binarystream.h"

//
// Cache-Manager
//
CacheManager* CacheManager::instance = nullptr;

CacheManager& CacheManager::get_instance() {
	if ( CacheManager::instance )
		return *instance;
	else
		throw NotInitializedException(
			"CacheManager was not initialized. Please use CacheManager::init first.");
}

void CacheManager::init( CacheManager *instance ) {
	CacheManager::instance = instance;
}

//
// NOP-Wrapper
//

template<typename T, CacheType CType>
NopCacheWrapper<T,CType>::NopCacheWrapper() {
}

template<typename T, CacheType CType>
bool NopCacheWrapper<T,CType>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query, const QueryProfiler &profiler) {
	(void) semantic_id;
	(void) item;
	(void) query;
	(void) profiler;
	return false;
}

template<typename T, CacheType CType>
std::unique_ptr<T> NopCacheWrapper<T,CType>::query(const GenericOperator& op,
		const QueryRectangle& rect, QueryProfiler &profiler) {
	(void) op;
	(void) rect;
	throw NoSuchElementException("NOP-Cache has no entries");
}

//
// NOP-Cache
//

NopCacheManager::NopCacheManager() {
}

CacheWrapper<GenericRaster>& NopCacheManager::get_raster_cache() {
	return raster_cache;
}

CacheWrapper<PointCollection>& NopCacheManager::get_point_cache() {
	return point_cache;
}

CacheWrapper<LineCollection>& NopCacheManager::get_line_cache() {
	return line_cache;
}

CacheWrapper<PolygonCollection>& NopCacheManager::get_polygon_cache() {
	return poly_cache;
}

CacheWrapper<GenericPlot>& NopCacheManager::get_plot_cache() {
	return plot_cache;
}

//
// Client Implementation
//


template<typename T, CacheType CType>
ClientCacheWrapper<T,CType>::ClientCacheWrapper(CacheType type, const std::string& idx_host,
		int idx_port) : type(type), idx_host(idx_host), idx_port(idx_port) {
}

template<typename T, CacheType CType>
bool ClientCacheWrapper<T,CType>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query, const QueryProfiler &profiler) {
	(void) semantic_id;
	(void) item;
	(void) query;
	(void) profiler;
	return false;
}

template<typename T, CacheType CType>
std::unique_ptr<T> ClientCacheWrapper<T,CType>::query(
		const GenericOperator& op, const QueryRectangle& rect, QueryProfiler &profiler) {

	try {
		BinaryFDStream idx_con(idx_host.c_str(), idx_port, true);

		BaseRequest req(type,op.getSemanticId(),rect);
		BinaryStream::buffered_write(idx_con, ClientConnection::MAGIC_NUMBER);
		BinaryStream::buffered_write(idx_con, ClientConnection::CMD_GET, req );

		auto resp = BinaryStream::buffered_read(idx_con);

		uint8_t idx_rc = resp->read<uint8_t>();
		switch (idx_rc) {
			case ClientConnection::RESP_OK: {
				DeliveryResponse dr(*resp);
				Log::debug("Contacting delivery-server: %s:%d, delivery_id: %d", dr.host.c_str(), dr.port, dr.delivery_id);

				BinaryFDStream del_sock(dr.host.c_str(),dr.port,true);

				BinaryStream::buffered_write( del_sock, DeliveryConnection::MAGIC_NUMBER);
				BinaryStream::buffered_write(del_sock,DeliveryConnection::CMD_GET, dr.delivery_id);

				auto del_resp = BinaryStream::buffered_read(del_sock);

				uint8_t del_rc = del_resp->read<uint8_t>();
				switch (del_rc) {
					case DeliveryConnection::RESP_OK: {
						Log::debug("Delivery responded OK.");
						return read_result(*del_resp);
					}
					case DeliveryConnection::RESP_ERROR: {
						std::string err_msg = del_resp->read<std::string>();
						Log::error("Delivery returned error: %s", err_msg.c_str());
						throw DeliveryException(err_msg);
					}
					default: {
						Log::error("Delivery returned unknown code: %d", del_rc);
						throw DeliveryException("Delivery returned unknown code");
					}
				}
				break;
			}
			case ClientConnection::RESP_ERROR: {
				std::string err_msg = resp->read<std::string>();
				Log::error("Cache returned error: %s", err_msg.c_str());
				throw OperatorException(err_msg);
			}
			default: {
				Log::error("Cache returned unknown code: %d", idx_rc);
				throw OperatorException("Cache returned unknown code");
			}
		}
	} catch ( const NetworkException &ne ) {
		Log::error("Could not connect to index-server: %s", ne.what());
		throw OperatorException(ne.what());
	}
}

template<typename T, CacheType CType>
std::unique_ptr<T> ClientCacheWrapper<T,CType>::read_result(
		BinaryStream& stream) {
	return make_unique<T>(stream);
}

template<>
std::unique_ptr<GenericRaster> ClientCacheWrapper<GenericRaster,CacheType::RASTER>::read_result(
		BinaryStream& stream) {
	return GenericRaster::fromStream(stream);
}

template<>
std::unique_ptr<GenericPlot> ClientCacheWrapper<GenericPlot,CacheType::PLOT>::read_result(
		BinaryStream& stream) {
	return GenericPlot::fromStream(stream);
}

//
// Client-cache
//

ClientCacheManager::ClientCacheManager(const std::string& idx_host, int idx_port) :
	idx_host(idx_host), idx_port(idx_port),
	raster_cache(CacheType::RASTER, idx_host, idx_port),
	point_cache(CacheType::POINT, idx_host, idx_port),
	line_cache(CacheType::LINE, idx_host, idx_port),
	poly_cache(CacheType::POLYGON, idx_host, idx_port),
	plot_cache(CacheType::PLOT, idx_host, idx_port){
}

CacheWrapper<GenericRaster>& ClientCacheManager::get_raster_cache() {
	return raster_cache;
}

CacheWrapper<PointCollection>& ClientCacheManager::get_point_cache() {
	return point_cache;
}

CacheWrapper<LineCollection>& ClientCacheManager::get_line_cache() {
	return line_cache;
}

CacheWrapper<PolygonCollection>& ClientCacheManager::get_polygon_cache() {
	return poly_cache;
}

CacheWrapper<GenericPlot>& ClientCacheManager::get_plot_cache() {
	return plot_cache;
}

template class NopCacheWrapper<GenericRaster,CacheType::RASTER> ;
template class NopCacheWrapper<PointCollection,CacheType::POINT> ;
template class NopCacheWrapper<LineCollection,CacheType::LINE> ;
template class NopCacheWrapper<PolygonCollection,CacheType::POLYGON> ;
template class NopCacheWrapper<GenericPlot,CacheType::PLOT> ;


template class ClientCacheWrapper<GenericRaster,CacheType::RASTER> ;
template class ClientCacheWrapper<PointCollection,CacheType::POINT> ;
template class ClientCacheWrapper<LineCollection,CacheType::LINE> ;
template class ClientCacheWrapper<PolygonCollection,CacheType::POLYGON> ;
template class ClientCacheWrapper<GenericPlot,CacheType::PLOT> ;
