/*
 * cacheclient.cpp
 *
 *  Created on: 28.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/client.h"
#include "cache/priv/connection.h"
#include "util/log.h"

CacheClient::CacheClient(std::string index_host, uint32_t index_port) :
		index_host(index_host), index_port(index_port) {
}

CacheClient::~CacheClient() {
}

std::unique_ptr<GenericRaster> CacheClient::get_raster(const std::string& graph_json,
		const QueryRectangle& query, const GenericOperator::RasterQM query_mode) {

	UnixSocket idx_con(index_host.c_str(), index_port);
	BinaryStream &stream = idx_con;
	uint32_t magic = ClientConnection::MAGIC_NUMBER;
	stream.write(magic);



	BaseRequest rr(graph_json,query);
	stream.write(ClientConnection::CMD_GET_RASTER);
	rr.toStream( stream );

	DeliveryResponse resp = read_index_response(idx_con);
	std::unique_ptr<GenericRaster> res = fetch_raster(resp);
	if ( query_mode == GenericOperator::RasterQM::EXACT )
		return res->fitToQueryRectangle(query);
	else
		return res;
}

DeliveryResponse CacheClient::read_index_response(BinaryStream& idx_con) {
	uint8_t idx_resp;
	idx_con.read(&idx_resp);
	switch (idx_resp) {
		case ClientConnection::RESP_OK: {
			return DeliveryResponse(idx_con);
		}
		case ClientConnection::RESP_ERROR: {
			std::string err_msg;
			idx_con.read(&err_msg);
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
	UnixSocket sock(dr.host.c_str(),dr.port);
	BinaryStream &stream = sock;

	stream.write(DeliveryConnection::MAGIC_NUMBER);
	stream.write(DeliveryConnection::CMD_GET);
	stream.write(dr.delivery_id);

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
