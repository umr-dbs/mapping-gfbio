/*
 * cache_manager.cpp
 *
 *  Created on: 15.06.2015
 *      Author: mika
 */

#include "cache/manager.h"
#include "cache/priv/transfer.h"
#include "cache/priv/connection.h"
#include "util/nio.h"

//
// STATICS
//
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
void NopCacheWrapper<T,CType>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query, QueryProfiler &profiler) {
	(void) semantic_id;
	(void) item;
	(void) query;
	(void) profiler;
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
void ClientCacheWrapper<T,CType>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle &query, QueryProfiler &profiler) {
	(void) semantic_id;
	(void) item;
	(void) query;
	(void) profiler;
}

template<typename T, CacheType CType>
std::unique_ptr<T> ClientCacheWrapper<T,CType>::query(
		const GenericOperator& op, const QueryRectangle& rect, QueryProfiler &profiler) {

	BinaryFDStream idx_con(idx_host.c_str(), idx_port, true);
	BinaryStream &idx_stream = idx_con;

	BaseRequest req(type,op.getSemanticId(),rect);
	buffered_write(idx_con, ClientConnection::MAGIC_NUMBER, ClientConnection::CMD_GET, req );

	uint8_t idx_resp;
	idx_stream.read(&idx_resp);
	switch (idx_resp) {
		case ClientConnection::RESP_OK: {
			DeliveryResponse dr(idx_con);
			Log::debug("Contacting delivery-server: %s:%d, delivery_id: %d", dr.host.c_str(), dr.port, dr.delivery_id);
			BinaryFDStream del_sock(dr.host.c_str(),dr.port,true);
			BinaryStream &del_stream = del_sock;

			buffered_write( del_sock, DeliveryConnection::MAGIC_NUMBER,DeliveryConnection::CMD_GET, dr.delivery_id);

			uint8_t resp;
			del_stream.read(&resp);
			switch (resp) {
				case DeliveryConnection::RESP_OK: {
					Log::debug("Delivery responded OK.");
					return read_result(del_sock);
				}
				case DeliveryConnection::RESP_ERROR: {
					std::string err_msg;
					del_stream.read(&err_msg);
					Log::error("Delivery returned error: %s", err_msg.c_str());
					throw DeliveryException(err_msg);
				}
				default: {
					Log::error("Delivery returned unknown code: %d", resp);
					throw DeliveryException("Delivery returned unknown code");
				}
			}
			break;
		}
		case ClientConnection::RESP_ERROR: {
			std::string err_msg;
			idx_stream.read(&err_msg);
			Log::error("Cache returned error: %s", err_msg.c_str());
			throw OperatorException(err_msg);
		}
		default: {
			Log::error("Cache returned unknown code: %d", idx_resp);
			throw OperatorException("Cache returned unknown code");
		}
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
