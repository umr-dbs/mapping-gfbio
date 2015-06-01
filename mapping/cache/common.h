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
#include <memory>

#include <sstream>
#include <cstring>
#include <sys/select.h>
#include <errno.h>

class CacheRequest {
public:
	virtual ~CacheRequest();
	CacheRequest( const CacheRequest &r );
	CacheRequest( const QueryRectangle &query, const std::string &graph_json );
	CacheRequest( BinaryStream &stream);
	virtual void toStream( BinaryStream &stream );
	QueryRectangle query;
	std::string graph_json;
};

class RasterRequest : public CacheRequest {
public:
	virtual ~RasterRequest();
	RasterRequest( const RasterRequest &r );
	RasterRequest( const QueryRectangle &query, const std::string &graph_json, GenericOperator::RasterQM query_mode );
	RasterRequest( BinaryStream &stream);
	virtual void toStream( BinaryStream &stream );
	GenericOperator::RasterQM query_mode;
};


class DeliveryResponse {
public:
	DeliveryResponse( const DeliveryResponse &r );
	DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id );
	DeliveryResponse( BinaryStream &stream );
	void toStream( BinaryStream &stream );
	std::string host;
	uint32_t port;
	uint64_t delivery_id;
};


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
	static const uint8_t CMD_INDEX_QUERY_CACHE = 4;

	//
	// Expected data on stream is:
	// QueryRectangle
	// OperatorGraph as JSON:string
	// RasterQM: uint8_t (1 == exact, 0 == loose)
	//
	static const uint8_t CMD_WORKER_GET_RASTER = 10;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// delivery_id:uint64_t
	//
	static const uint8_t CMD_DELIVERY_GET = 20;


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
	// host:string
	// port:uint32_t
	// delivery_id:uint64_t
	static const uint8_t RESP_INDEX_HIT = 32;

	//
	// Response from index-server after unsuccessfuly
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Theres no data on stream.
	static const uint8_t RESP_INDEX_MISS = 33;

	//
	// Response for ready to deliver response. Data on stream is:
	// delivery_id:uint64_t
	//
	static const uint8_t RESP_WORKER_RESULT_READY = 40;

	//
	// Send if a new entry is added to the local cache
	//
	static const uint8_t RESP_WORKER_NEW_CACHE_ENTRY = 41;

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
	// reason:string
	//
	static const uint8_t RESP_DELIVERY_ERROR = 80;


	//
	// Creates a listening socket on the given port.
	//
	static int getListeningSocket(int port, bool nonblock = true, int backlog = 10);

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


private:
	Common() {};
	~Common() {};
};

#endif /* COMMON_H_ */
