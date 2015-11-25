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

	auto res = GenericRaster::fromStream( *process_request(CacheType::RASTER, query, graph_json) );
	if ( query_mode == GenericOperator::RasterQM::EXACT )
		return res->fitToQueryRectangle(query);
	else
		return res;
}

std::unique_ptr<PointCollection> CacheClient::get_pointcollection(const std::string& graph_json,
	const QueryRectangle& query, const GenericOperator::FeatureCollectionQM query_mode) {

	return get_feature_collection<PointCollection>( CacheType::POINT, graph_json, query, query_mode );
}

std::unique_ptr<LineCollection> CacheClient::get_linecollection(const std::string& graph_json,
	const QueryRectangle& query, const GenericOperator::FeatureCollectionQM query_mode) {

	return get_feature_collection<LineCollection>( CacheType::LINE, graph_json, query, query_mode );
}

std::unique_ptr<PolygonCollection> CacheClient::get_polygoncollection(const std::string& graph_json,
	const QueryRectangle& query, const GenericOperator::FeatureCollectionQM query_mode) {

	return get_feature_collection<PolygonCollection>( CacheType::POLYGON, graph_json, query, query_mode );
}

std::unique_ptr<GenericPlot> CacheClient::get_plot(const std::string& graph_json,
	const QueryRectangle& query) {

	return GenericPlot::fromStream( *process_request(CacheType::PLOT, query, graph_json) );
}


template<typename T>
std::unique_ptr<T> CacheClient::get_feature_collection( CacheType type, const std::string& graph_json,
	const QueryRectangle& query, const GenericOperator::FeatureCollectionQM query_mode) {

	auto res = make_unique<T>( *process_request(type, query, graph_json) );
	if (query_mode == GenericOperator::FeatureCollectionQM::SINGLE_ELEMENT_FEATURES && !res->isSimple())
		throw OperatorException("Operator did not return Features consisting only of single points");
	else
		return res;
}

std::unique_ptr<UnixSocket> CacheClient::process_request( CacheType type, const QueryRectangle& query, const std::string& workflow ) {

	UnixSocket idx_con(index_host.c_str(), index_port);
	BinaryStream &stream = idx_con;
	uint32_t magic = ClientConnection::MAGIC_NUMBER;
	stream.write(magic);

	BaseRequest rr(type,workflow,query);
	stream.write(ClientConnection::CMD_GET);
	rr.toStream( stream );

	uint8_t idx_resp;
	stream.read(&idx_resp);
	switch (idx_resp) {
		case ClientConnection::RESP_OK: {
			DeliveryResponse dr(idx_con);
			Log::debug("Contacting delivery-server: %s:%d, delivery_id: %d", dr.host.c_str(), dr.port, dr.delivery_id);
			auto dsock = make_unique<UnixSocket>(dr.host.c_str(),dr.port);
			BinaryStream &dstream = *dsock;

			dstream.write(DeliveryConnection::MAGIC_NUMBER);
			dstream.write(DeliveryConnection::CMD_GET);
			dstream.write(dr.delivery_id);

			uint8_t resp;
			dstream.read(&resp);
			switch (resp) {
				case DeliveryConnection::RESP_OK: {
					Log::debug("Delivery responded OK.");
					return dsock;
				}
				case DeliveryConnection::RESP_ERROR: {
					std::string err_msg;
					dstream.read(&err_msg);
					Log::error("Delivery returned error: %s", err_msg.c_str());
					throw DeliveryException(err_msg);
				}
				default: {
					Log::error("Delivery returned unknown code: %d", resp);
					throw DeliveryException("Delivery returned unknown code");
				}
			}
		}
		case ClientConnection::RESP_ERROR: {
			std::string err_msg;
			stream.read(&err_msg);
			Log::error("Cache returned error: %s", err_msg.c_str());
			throw OperatorException(err_msg);
		}
		default: {
			Log::error("Cache returned unknown code: %d", idx_resp);
			throw OperatorException("Cache returned unknown code");
		}
	}
}
