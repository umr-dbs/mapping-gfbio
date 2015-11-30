/*
 * common.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/priv/cache_structure.h"
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
#include <execinfo.h>


void ex_handler() {
	std::ostringstream out;

	std::exception_ptr exptr = std::current_exception();
	try {
	    std::rethrow_exception(exptr);
	}
	catch (std::exception &ex) {
	    out << "Uncaught exception: " << ex.what();
	}


	Log::error("%s\n%s",out.str().c_str(), CacheCommon::get_stacktrace().c_str() );
	exit(1);
}

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

void CacheCommon::set_uncaught_exception_handler() {
	std::set_terminate(ex_handler);
}

std::string CacheCommon::get_stacktrace() {
	std::ostringstream out;
	void *trace_elems[20];
	int trace_elem_count(backtrace(trace_elems, 20));
	char **stack_syms(backtrace_symbols(trace_elems, trace_elem_count));
	for (int i = 0; i < trace_elem_count; ++i) {
		out << stack_syms[i] << std::endl;
	}
	free(stack_syms);
	return out.str();
}

bool CacheCommon::resolution_matches(const GridSpatioTemporalResult& r1,
		const GridSpatioTemporalResult& r2) {
	return resolution_matches(r1.pixel_scale_x, r1.pixel_scale_y, r2.pixel_scale_x, r2.pixel_scale_y );
}

bool CacheCommon::resolution_matches(const CacheCube &c1, const CacheCube &c2) {
	return resolution_matches( c1.resolution_info.actual_pixel_scale_x,
		c1.resolution_info.actual_pixel_scale_y,
		c2.resolution_info.actual_pixel_scale_x,
		c2.resolution_info.actual_pixel_scale_y);
}

bool CacheCommon::resolution_matches(double scalex1, double scaley1,
		double scalex2, double scaley2) {

	return std::abs( 1.0 - scalex1 / scalex2) < 0.01 &&
		   std::abs( 1.0 - scaley1 / scaley2) < 0.01;
}
