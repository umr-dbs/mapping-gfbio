/*
 * common.h
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#ifndef COMMON_H_
#define COMMON_H_

#include "util/binarystream.h"
#include "raster/exceptions.h"
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
// Wraps a BinaryStream and stores the underlying
// FileDescriptor for use in select().
//
class SocketConnection {
public:
	SocketConnection( const SocketConnection & ) = delete;
	SocketConnection operator=( const SocketConnection & ) = delete;
	SocketConnection(int fd);
	SocketConnection(const char *host, int port);
	~SocketConnection();
	int fd;
	std::unique_ptr<BinaryStream> stream;
};

//
// Provides helper functions for common tasks and
// all command and response codes used by the cache.
//
class Common {
public:

	//////////////////////////////////////////////////
	//
	// COMMANDS
	//
	//////////////////////////////////////////////////

	//
	// Used by nodes to register at the index-server
	// Expected data on stream is:
	// node hostname:string
	// node delivery port:uint32_t
	//
	static const uint8_t CMD_INDEX_NODE_HELLO = 1;

	//
	// Used by node-workers to register at the index-server
	// Expected data on stream is:
	// id:uint32_t -- the id received with RESP_INDEX_NODE_HELLO
	//
	static const uint8_t CMD_INDEX_REGISTER_WORKER = 2;

	//
	// Expected data on stream is:
	// QueryRectangle
	// OperatorGraph as JSON:string
	// RasterQM: uint8_t (1 == exact, 0 == loose)
	//
	static const uint8_t CMD_INDEX_GET_RASTER = 3;

	//
	// Expected data on stream is:
	// QueryRectangle
	// OperatorGraph as JSON:string
	//
	static const uint8_t CMD_INDEX_QUERY_RASTER_CACHE = 4;

	//
	// Expected data on stream is:
	// request:RasterBaseRequest
	//
	static const uint8_t CMD_WORKER_CREATE_RASTER = 10;

	//
	// Expected data on stream is:
	// request:RasterDeliveryRequest
	//
	static const uint8_t CMD_WORKER_DELIVER_RASTER = 11;

	//
	// Expected data on stream is:
	// request:RasterPuzzleRequest
	//
	static const uint8_t CMD_WORKER_PUZZLE_RASTER = 12;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// delivery_id:uint64_t
	//
	static const uint8_t CMD_DELIVERY_GET = 20;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// key:STCacheKey
	//
	static const uint8_t CMD_DELIVERY_GET_CACHED_RASTER = 21;


	//////////////////////////////////////////////////
	//
	// RESPONSES
	//
	//////////////////////////////////////////////////

	//
	// Response from index-server after successful
	// registration of a new node. Data on stream is:
	// id:uint32_t -- the id assigned to the node
	static const uint8_t RESP_INDEX_NODE_HELLO = 30;

	//
	// Response from index-server after successfully
	// processing a request. Data on stream is:
	// host:string
	// port:uint32_t
	// delivery_id:uint64_t
	static const uint8_t RESP_INDEX_GET = 31;

	//
	// Response from index-server after successfully
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Data on stream is:
	// ref:CacheRef
	static const uint8_t RESP_INDEX_HIT = 32;

	//
	// Response from index-server after unsuccessfuly
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Theres no data on stream.
	static const uint8_t RESP_INDEX_MISS = 33;

	//
	// Response from index-server after successfully
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Data on stream is:
	// puzzle-request: PuzzleRequest
	static const uint8_t RESP_INDEX_PARTIAL = 34;


	//
	// Response for ready to deliver result. Data on stream is:
	// delivery-id:uint64_t
	//
	static const uint8_t RESP_WORKER_RESULT_READY = 40;

	//
	// Send if a new raster-entry is added to the local cache
	// Data on stream is:
	// key:STCacheKey
	// cube:RasterCacheCube
	//
	static const uint8_t RESP_WORKER_NEW_RASTER_CACHE_ENTRY = 41;

	//
	// Response if delivery is send. Data:
	// GenericRaster
	//
	static const uint8_t RESP_DELIVERY_OK = 50;

	//////////////////////////////////////////////////
	//
	// ERRORS
	//
	//////////////////////////////////////////////////

	//
	// Returned on errors by the index-server.
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_INDEX_ERROR = 60;

	//
	// Send if a worker cannot fulfill the request
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_WORKER_ERROR = 70;

	//
	// Response if delivery faild. Data:
	// message:string -- a description of the error
	//
	static const uint8_t RESP_DELIVERY_ERROR = 80;


	//
	// Creates a listening socket on the given port.
	//
	static int get_listening_socket(int port, bool nonblock = true, int backlog = 10);

	static std::string qr_to_string( const QueryRectangle &rect );

	static std::string stref_to_string( const SpatioTemporalReference &ref );

	//
	// Fetches a raster directly from the delivery-manager of the given node
	// by passing the unique STCacheKey
	//
	static std::unique_ptr<GenericRaster> fetch_raster(const std::string & host, uint32_t port, const STCacheKey &key );

	//
	// Puzzles a raster by combinig parts
	//
	static std::unique_ptr<GenericRaster> process_raster_puzzle(const PuzzleRequest &req, std::string my_host,
		uint32_t my_port);

	//
	// Helper to read from a stream with a given timeout. Basically wraps
	// BinaryStream::read(T*,bool).
	// If the timeout is reached, a TimeoutException is thrown. If select()
	// gets interrupted an InterruptedException is thrown. Both are not
	// harmful to the underlying connection.
	// On a harmful error, a NetworkException is thrown.
	//
	template<typename T>
	static size_t read(T *t, SocketConnection &con, int timeout, bool allow_eof = false) {
		struct timeval tv { timeout, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(con.fd, &readfds);

		int ret = select(con.fd + 1, &readfds, nullptr, nullptr, &tv);
		if ( ret > 0 )
			return con.stream->read(t,allow_eof);
		else if ( ret == 0 )
			throw TimeoutException("No data available");
		else if ( errno == EINTR )
			throw InterruptedException("Select interrupted");
		else {
			std::ostringstream msg;
			msg << "UnixSocket: read() failed: " << strerror(errno);
			throw NetworkException(msg.str());
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
	Common() {};
	~Common() {};
};

#endif /* COMMON_H_ */
