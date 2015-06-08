/*
 * common.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "util/log.h"
#include "raster/exceptions.h"

#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <errno.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

std::string Common::qr_to_string(const QueryRectangle &rect) {
	std::ostringstream os;
	os << "QueryRectangle[ epsg: " << (uint16_t) rect.epsg << ", timestamp: "
			<< rect.timestamp << ", x: [" << rect.x1 << "," << rect.x2 << "]"
			<< ", y: [" << rect.y1 << "," << rect.y2 << "]" << ", res: ["
			<< rect.xres << "," << rect.yres << "] ]";
	return os.str();
}

std::string Common::stref_to_string(const SpatioTemporalReference &ref) {
	std::ostringstream os;
	os << "SpatioTemporalReference[ epsg: " << (uint16_t) ref.epsg
			<< ", timetype: " << (uint16_t) ref.timetype << ", time: ["
			<< ref.t1 << "," << ref.t2 << "]" << ", x: [" << ref.x1 << ","
			<< ref.x2 << "]" << ", y: [" << ref.y1 << "," << ref.y2 << "] ]";
	return os.str();
}

int Common::get_listening_socket(int port, bool nonblock, int backlog) {
	int sock;
	struct addrinfo hints, *servinfo, *p;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	char portstr[16];
	snprintf(portstr, 16, "%d", port);
	int rv;
	if ((rv = getaddrinfo(nullptr, portstr, &hints, &servinfo)) != 0) {
		throw NetworkException("getaddrinfo() failed");
	}

	// loop through all the results and bind to the first we can
	for (p = servinfo; p != nullptr; p = p->ai_next) {
		int type = nonblock ? p->ai_socktype | nonblock : p->ai_socktype;

		if ((sock = socket(p->ai_family, type,
				p->ai_protocol)) == -1)
			continue;

		int yes = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
				== -1) {
			freeaddrinfo(servinfo); // all done with this structure
			throw NetworkException("setsockopt() failed");
		}

		if (bind(sock, p->ai_addr, p->ai_addrlen) == -1) {
			close(sock);
			continue;
		}
		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (p == nullptr)
		throw NetworkException("failed to bind");

	if (listen(sock, backlog) == -1)
		throw NetworkException("listen() failed");

	return sock;
}

std::unique_ptr<GenericRaster> Common::fetch_raster(const DeliveryResponse& dr) {
	Log::debug("Fetching raster from: %s:%d", dr.host.c_str(), dr.port);
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


std::unique_ptr<GenericRaster> Common::fetch_raster(const std::string & host, uint32_t port,
		const STCacheKey &key ) {
	Log::debug("Fetching cache-entry from: %s:%d", host.c_str(), port);
	SocketConnection dc(host.c_str(),port);
	uint8_t cmd = Common::CMD_DELIVERY_GET_CACHED_RASTER;
	dc.stream->write(cmd);
	key.toStream(*dc.stream);

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


//
// Connection class
//
SocketConnection::SocketConnection(int fd) : fd(fd) {
	stream.reset(new UnixSocket(fd, fd));
};

SocketConnection::SocketConnection(const char* host, int port) : fd(-1) {
	UnixSocket *sck = new UnixSocket(host,port);
	fd = sck->getReadFD();
	stream.reset( sck );
}

SocketConnection::~SocketConnection() {
}

//
// Request/response classes
//

CacheRequest::CacheRequest(const CacheRequest& r) : query(r.query), semantic_id(r.semantic_id) {
}

CacheRequest::CacheRequest(const QueryRectangle& query, const std::string& graph_json) :
	query(query), semantic_id( GenericOperator::fromJSON(graph_json)->getSemanticId() ){
}

CacheRequest::CacheRequest(BinaryStream &stream) :
	query(stream) {
	std::string graph_json;
	stream.read(&graph_json);
	Log::debug("Read graph-json: %s", graph_json.c_str());
	auto op = GenericOperator::fromJSON(graph_json);
	semantic_id = op->getSemanticId();
}

CacheRequest::~CacheRequest() {
}

void CacheRequest::toStream(BinaryStream& stream) {
	query.toStream(stream);
	stream.write(semantic_id);
}


RasterRequest::RasterRequest(const RasterRequest& r) : CacheRequest(r), query_mode(r.query_mode){
}

RasterRequest::RasterRequest(const QueryRectangle& query, const std::string& graph_json,
		GenericOperator::RasterQM query_mode) : CacheRequest(query,graph_json), query_mode(query_mode) {
}

RasterRequest::RasterRequest(BinaryStream& stream) : CacheRequest(stream) {
	uint8_t qm;
	stream.read(&qm);
	query_mode = (qm == 1) ? GenericOperator::RasterQM::EXACT : GenericOperator::RasterQM::LOOSE;
}

void RasterRequest::toStream(BinaryStream& stream) {
	uint8_t qm = (query_mode == GenericOperator::RasterQM::EXACT) ? 1 : 0;
	CacheRequest::toStream(stream);
	stream.write(qm);
}

RasterRequest::~RasterRequest() {
}


DeliveryResponse::DeliveryResponse(const DeliveryResponse& r) :
	host(r.host), port(r.port), delivery_id(r.delivery_id) {
}

DeliveryResponse::DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id) :
			host(host), port(port), delivery_id(delivery_id) {
}

DeliveryResponse::DeliveryResponse(BinaryStream& stream) {
	stream.read(&host);
	stream.read(&port);
	stream.read(&delivery_id);
}

void DeliveryResponse::toStream(BinaryStream& stream) {
	stream.write(host);
	stream.write(port);
	stream.write(delivery_id);
}
