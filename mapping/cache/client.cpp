/*
 * cacheclient.cpp
 *
 *  Created on: 28.05.2015
 *      Author: mika
 */

#include "cache/client.h"
#include "cache/common.h"
#include "util/log.h"

CacheClient::CacheClient(std::string index_host, uint32_t index_port) :
		index_host(index_host), index_port(index_port) {
}

CacheClient::~CacheClient() {
}

std::unique_ptr<GenericRaster> CacheClient::get_raster(const std::string& graph_json,
		const QueryRectangle& query, const GenericOperator::RasterQM query_mode) {

	SocketConnection idx_con(index_host.c_str(), index_port);

	uint8_t idx_cmd = Common::CMD_INDEX_GET_RASTER;
	RasterRequest rr(query,graph_json,query_mode);
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

std::unique_ptr<GenericRaster> CacheClient::fetch_raster(const DeliveryResponse &resp) {

	SocketConnection del_con(resp.host.c_str(), resp.port);

	uint8_t del_cmd = Common::CMD_DELIVERY_GET;
	del_con.stream->write(del_cmd);
	del_con.stream->write(resp.delivery_id);

	uint8_t del_resp;
	del_con.stream->read(&del_resp);
	switch (del_resp) {
		case Common::RESP_DELIVERY_OK: {
			return GenericRaster::fromStream(*del_con.stream);
		}
		case Common::RESP_DELIVERY_ERROR: {
			std::string err_msg;
			del_con.stream->read(&err_msg);
			Log::error("Delivery returned error: %s", err_msg.c_str());
			throw OperatorException(err_msg);
		}
		default: {
			Log::error("Cache returned unknown code: %d", del_resp);
			throw OperatorException("Delivery returned unknown code");
		}
	}
}
