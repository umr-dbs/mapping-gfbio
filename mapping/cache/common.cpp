/*
 * common.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/priv/types.h"
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

geos::geom::GeometryFactory Common::gf;

std::string Common::qr_to_string(const QueryRectangle &rect) {
	std::ostringstream os;
	os << "QueryRectangle[ epsg: " << (uint16_t) rect.epsg << ", timestamp: " << rect.timestamp << ", x: ["
		<< rect.x1 << "," << rect.x2 << "]" << ", y: [" << rect.y1 << "," << rect.y2 << "]" << ", res: ["
		<< rect.xres << "," << rect.yres << "] ]";
	return os.str();
}

std::string Common::stref_to_string(const SpatioTemporalReference &ref) {
	std::ostringstream os;
	os << "SpatioTemporalReference[ epsg: " << (uint16_t) ref.epsg << ", timetype: "
		<< (uint16_t) ref.timetype << ", time: [" << ref.t1 << "," << ref.t2 << "]" << ", x: [" << ref.x1
		<< "," << ref.x2 << "]" << ", y: [" << ref.y1 << "," << ref.y2 << "] ]";
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

		if ((sock = socket(p->ai_family, type, p->ai_protocol)) == -1)
			continue;

		int yes = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
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

std::unique_ptr<GenericRaster> Common::fetch_raster(const std::string & host, uint32_t port,
	const STCacheKey &key) {
	Log::debug("Fetching cache-entry from: %s:%d, key: %s", host.c_str(), port, key.to_string().c_str());
	SocketConnection dc(host.c_str(), port);
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

std::unique_ptr<GenericRaster> Common::process_raster_puzzle(const PuzzleRequest& req, std::string my_host,
	uint32_t my_port) {
	typedef std::unique_ptr<GenericRaster> RP;
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
			items.push_back( CacheManager::getInstance().get_raster(req.semantic_id, cr.entry_id) );
		}
		else {
			Log::debug("Fetching puzzle-piece from %s:%d, key: %s:%d", cr.host.c_str(), cr.port,
				req.semantic_id.c_str(), cr.entry_id);
			items.push_back( fetch_raster(cr.host, cr.port, STCacheKey(req.semantic_id, cr.entry_id)) );
		}
	}

	// Create remainder
	if (!req.remainder->isEmpty()) {
		Log::trace("Creating remainder: %s", req.remainder->toString().c_str());
		auto graph = GenericOperator::fromJSON(req.semantic_id);
		QueryProfiler qp;
		auto &f = items.at(0);

		QueryRectangle rqr = req.get_remainder_query(f->pixel_scale_x,f->pixel_scale_y);
		RP rem = graph->getCachedRaster(rqr, qp, GenericOperator::RasterQM::LOOSE);

		if ( std::abs( 1.0 - f->pixel_scale_x / rem->pixel_scale_x) > 0.01 ||
			 std::abs( 1.0 - f->pixel_scale_y / rem->pixel_scale_y) > 0.01 ) {
			Log::error("Resolution clash on remainder. Requires: [%f,%f], result: [%f,%f], QueryRectangle: [%f,%f], %s",
				f->pixel_scale_x, f->pixel_scale_y, rem->pixel_scale_x, rem->pixel_scale_y,
				((rqr.x2-rqr.x1) / rqr.xres), ((rqr.y2-rqr.y1) / rqr.yres), qr_to_string(rqr).c_str());

			throw OperatorException("Incompatible resolution on remainder");
		}

		STRasterEntryBounds rcc(*rem);
		GP cube_square = Common::create_square(rcc.x1, rcc.y1, rcc.x2, rcc.y2);
		covered = GP(covered->Union(cube_square.get()));
		items.push_back(std::move(rem));
	}

	RP result = CacheManager::do_puzzle(req.query, *covered, items );
	Log::trace("Finished processing puzzle-request: %s", req.to_string().c_str());
	return result;
}

std::unique_ptr<geos::geom::Polygon> Common::create_square(double lx, double ly, double ux, double uy) {
	geos::geom::CoordinateSequence *coords = new geos::geom::CoordinateArraySequence();

	coords->add(geos::geom::Coordinate(lx, ly));
	coords->add(geos::geom::Coordinate(ux, ly));
	coords->add(geos::geom::Coordinate(ux, uy));
	coords->add(geos::geom::Coordinate(lx, uy));
	coords->add(geos::geom::Coordinate(lx, ly));

	geos::geom::LinearRing *shell = gf.createLinearRing(coords);
	std::vector<geos::geom::Geometry*> *empty = new std::vector<geos::geom::Geometry*>;
	return std::unique_ptr<geos::geom::Polygon>(gf.createPolygon(shell, empty));
}

std::unique_ptr<geos::geom::Geometry> Common::empty_geom() {
	return std::unique_ptr<geos::geom::Geometry>(gf.createEmptyGeometry());
}

std::unique_ptr<geos::geom::Geometry> Common::union_geom(const std::unique_ptr<geos::geom::Geometry>& p1,
	const std::unique_ptr<geos::geom::Polygon>& p2) {
	return std::unique_ptr<geos::geom::Geometry>(p1->Union(p2.get()));
}

//
// Connection class
//
SocketConnection::SocketConnection(int fd) :
	fd(fd) {
	stream.reset(new UnixSocket(fd, fd));
}
;

SocketConnection::SocketConnection(const char* host, int port) :
	fd(-1) {
	UnixSocket *sck = new UnixSocket(host, port);
	fd = sck->getReadFD();
	stream.reset(sck);
}

SocketConnection::~SocketConnection() {
}
