/*
 * common.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/priv/connection.h"
#include "util/log.h"
#include "util/exceptions.h"

#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <errno.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

geos::geom::GeometryFactory CacheCommon::gf;

std::string CacheCommon::qr_to_string(const QueryRectangle &rect) {
	std::ostringstream os;
	os << "QueryRectangle[ epsg: " << (uint16_t) rect.epsg << ", time: " << rect.t1 << " - " << rect.t2 << ", x: ["
		<< rect.x1 << "," << rect.x2 << "]" << ", y: [" << rect.y1 << "," << rect.y2 << "]" << ", res: ["
		<< rect.xres << "," << rect.yres << "] ]";
	return os.str();
}

std::string CacheCommon::stref_to_string(const SpatioTemporalReference &ref) {
	std::ostringstream os;
	os << "SpatioTemporalReference[ epsg: " << (uint16_t) ref.epsg << ", timetype: "
		<< (uint16_t) ref.timetype << ", time: [" << ref.t1 << "," << ref.t2 << "]" << ", x: [" << ref.x1
		<< "," << ref.x2 << "]" << ", y: [" << ref.y1 << "," << ref.y2 << "] ]";
	return os.str();
}


std::string CacheCommon::raster_to_string(const GenericRaster& raster) {

	return concat( "GenericRaster: ", stref_to_string(raster.stref),
		", size: ", raster.width, "x", raster.height,
		", res: ", raster.pixel_scale_x, "x", raster.pixel_scale_y, "]");
}


int CacheCommon::get_listening_socket(int port, bool nonblock, int backlog) {
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

std::unique_ptr<geos::geom::Polygon> CacheCommon::create_square(double lx, double ly, double ux, double uy) {
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

std::unique_ptr<geos::geom::Geometry> CacheCommon::empty_geom() {
	return std::unique_ptr<geos::geom::Geometry>(gf.createEmptyGeometry());
}

std::unique_ptr<geos::geom::Geometry> CacheCommon::union_geom(const std::unique_ptr<geos::geom::Geometry>& p1,
	const std::unique_ptr<geos::geom::Polygon>& p2) {
	return std::unique_ptr<geos::geom::Geometry>(p1->Union(p2.get()));
}
