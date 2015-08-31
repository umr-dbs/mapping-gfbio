/*
 * common.h
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#ifndef COMMON_H_
#define COMMON_H_

#include "util/binarystream.h"
#include "util/exceptions.h"
#include "operators/operator.h"
#include "datatypes/raster.h"
#include "cache/priv/transfer.h"
#include "cache/manager.h"
#include <memory>

#include <sstream>
#include <cstring>
#include <sys/select.h>
#include <errno.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>

//
// Provides helper functions for common tasks.
//
class CacheCommon {
public:

	//
	// Creates a listening socket on the given port.
	//
	static int get_listening_socket(int port, bool nonblock = true, int backlog = 10);

	//
	// Returns a string-representation for the given query-rectange
	//
	static std::string qr_to_string( const QueryRectangle &rect );

	//
	// Returns a string-representation for the given spatio-temporal reference
	//
	static std::string stref_to_string( const SpatioTemporalReference &ref );

	//
	// Returns a string-representation of the given raster
	//
	static std::string raster_to_string( const GenericRaster &raster );

	//
	// Helper to read from a stream with a given timeout. Basically wraps
	// BinaryStream::read(T*,bool).
	// If the timeout is reached, a TimeoutException is thrown. If select()
	// gets interrupted an InterruptedException is thrown. Both are not
	// harmful to the underlying connection.
	// On a harmful error, a NetworkException is thrown.
	//
	template<typename T>
	static size_t read(T *t, UnixSocket &sock, int timeout, bool allow_eof = false) {
		struct timeval tv { timeout, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(sock.getReadFD(), &readfds);
		BinaryStream &stream = sock;

		int ret = select(sock.getReadFD()+1, &readfds, nullptr, nullptr, &tv);
		if ( ret > 0 )
			return stream.read(t,allow_eof);
		else if ( ret == 0 )
			throw TimeoutException("No data available");
		else if ( errno == EINTR )
			throw InterruptedException("Select interrupted");
		else {
			throw NetworkException(concat("UnixSocket: read() failed: ", strerror(errno)));
		}
	}

	//
	// Geos Helper functions used by the caches query function
	//
	static std::unique_ptr<geos::geom::Geometry> empty_geom();
	static std::unique_ptr<geos::geom::Polygon> create_square( double lx, double ly, double ux, double uy );
	static std::unique_ptr<geos::geom::Geometry> union_geom( const std::unique_ptr<geos::geom::Geometry> &p1,
															   const std::unique_ptr<geos::geom::Polygon> &p2);


private:
	static geos::geom::GeometryFactory gf;
	CacheCommon() {};
	~CacheCommon() {};
};

#endif /* COMMON_H_ */
