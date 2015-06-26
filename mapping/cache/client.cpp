/*
 * cacheclient.cpp
 *
 *  Created on: 28.05.2015
 *      Author: mika
 */

#include "cache/client.h"
#include "cache/common.h"
#include "cache/index/connection.h"
#include "util/log.h"

CacheClient::CacheClient(std::string index_host, uint32_t index_port) :
		index_host(index_host), index_port(index_port) {
}

CacheClient::~CacheClient() {
}

std::unique_ptr<GenericRaster> CacheClient::get_raster(const std::string& graph_json,
		const QueryRectangle& query, const GenericOperator::RasterQM query_mode) {

	SocketConnection idx_con(index_host.c_str(), index_port);
	uint32_t magic = ClientConnection::MAGIC_NUMBER;
	idx_con.stream->write(magic);



	uint8_t idx_cmd = Common::CMD_INDEX_GET_RASTER;
	RasterBaseRequest rr(graph_json,query,query_mode);
	idx_con.stream->write(idx_cmd);
	rr.toStream( *idx_con.stream );

	DeliveryResponse resp = read_index_response(idx_con);
	return fetch_raster(resp);
}

DeliveryResponse CacheClient::read_index_response(SocketConnection& idx_con) {
	uint8_t idx_resp;
	idx_con.stream->read(&idx_resp);
	switch (idx_resp) {
		case Common::RESP_INDEX_GET: {
			return DeliveryResponse(*idx_con.stream);
		}
		case Common::RESP_INDEX_ERROR: {
			std::string err_msg;
			idx_con.stream->read(&err_msg);
			Log::error("Cache returned error: %s", err_msg.c_str());
			throw OperatorException(err_msg);
		}
		default: {
			Log::error("Cache returned unknown code: %d", idx_resp);
			throw OperatorException("Cache returned unknown code");
		}
	}
}

std::unique_ptr<GenericRaster> CacheClient::fetch_raster(const DeliveryResponse& dr) {
	Log::debug("Fetching raster from: %s:%d, delivery_id: %d", dr.host.c_str(), dr.port, dr.delivery_id);
	SocketConnection dc(dr.host.c_str(),dr.port);
	uint8_t cmd = Common::CMD_DELIVERY_GET;
	dc.stream->write(cmd);
	dc.stream->write(dr.delivery_id);

	uint8_t resp;
	dc.stream->read(&resp);
	switch (resp) {
		case Common::RESP_DELIVERY_OK: {
			return GenericRaster::fromStream(*dc.stream);
		}
		case Common::RESP_DELIVERY_ERROR: {
			std::string err_msg;
			dc.stream->read(&err_msg);
			Log::error("Delivery returned error: %s", err_msg.c_str());
			throw DeliveryException(err_msg);
		}
		default: {
			Log::error("Delivery returned unknown code: %d", resp);
			throw DeliveryException("Delivery returned unknown code");
		}
	}
}
