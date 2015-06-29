/*
 * connection.h
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "util/binarystream.h"
#include "cache/cache.h"
#include "cache/priv/transfer.h"

#include <map>
#include <memory>

class Node;

class BaseConnection {
public:
	BaseConnection(std::unique_ptr<UnixSocket> &socket);
	virtual ~BaseConnection();
	int get_read_fd();
	const uint64_t id;
	void input();
	bool is_faulty();
protected:
	virtual void process_command( uint8_t cmd ) = 0;
	BinaryStream &stream;
	bool faulty;
private:
	std::unique_ptr<UnixSocket> socket;
	static uint64_t next_id;
};

class ClientConnection: public BaseConnection {
public:
	enum class State {
		IDLE, AWAIT_RESPONSE
	};
	enum class RequestType {
		NONE, RASTER, POINT, LINE, POLY, PLOT
	};

	static const uint32_t MAGIC_NUMBER = 0x22345678;

	//
	// Expected data on stream is:
	// RasterBaseRequest
	//
	static const uint8_t CMD_GET_RASTER = 1;

	//
	// Response from index-server after successfully
	// processing a request. Data on stream is:
	// DeliveryResponse
	static const uint8_t RESP_OK = 10;

	//
	// Returned on errors by the index-server.
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_ERROR = 19;

	ClientConnection(std::unique_ptr<UnixSocket> &socket);
	virtual ~ClientConnection();

	State get_state() const;

	// Sends the given response and resets the state to IDLE
	void send_response(const DeliveryResponse &response);
	// Sends the given error and resets the state to IDLE
	void send_error(const std::string &message);

	RequestType get_request_type() const;
	const RasterBaseRequest& get_raster_request() const;

protected:
	virtual void process_command( uint8_t cmd );

private:
	void reset();
	State state;
	RequestType request_type;
	std::unique_ptr<RasterBaseRequest> raster_request;
};

class WorkerConnection: public BaseConnection {
public:
	enum class State {
		IDLE, PROCESSING, NEW_RASTER_ENTRY, RASTER_QUERY_REQUESTED, DONE, ERROR
	};
	static const uint32_t MAGIC_NUMBER = 0x32345678;

	//
	// Expected data on stream is:
	// request:RasterBaseRequest
	//
	static const uint8_t CMD_CREATE_RASTER = 20;

	//
	// Expected data on stream is:
	// request:RasterDeliveryRequest
	//
	static const uint8_t CMD_DELIVER_RASTER = 21;

	//
	// Expected data on stream is:
	// request:RasterPuzzleRequest
	//
	static const uint8_t CMD_PUZZLE_RASTER = 22;

	//
	// Expected data on stream is:
	// BaseRequest
	//
	static const uint8_t CMD_QUERY_RASTER_CACHE = 23;

	//
	// Response for ready to deliver result. Data on stream is:
	// delivery-id:uint64_t
	//
	static const uint8_t RESP_RESULT_READY = 30;

	//
	// Send if a new raster-entry is added to the local cache
	// Data on stream is:
	// key:STCacheKey
	// cube:RasterCacheCube
	//
	static const uint8_t RESP_NEW_RASTER_CACHE_ENTRY = 31;

	//
	// Response from index-server after successfully
	// probing the cache for a RESP_QUERY_RASTER_CACHE.
	// Data on stream is:
	// ref:CacheRef
	static const uint8_t RESP_QUERY_HIT = 32;

	//
	// Response from index-server after unsuccessfuly
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Theres no data on stream.
	static const uint8_t RESP_QUERY_MISS = 33;

	//
	// Response from index-server after successfully
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Data on stream is:
	// puzzle-request: PuzzleRequest
	static const uint8_t RESP_QUERY_PARTIAL = 34;

	//
	// Send if a worker cannot fulfill the request
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_ERROR = 39;

	WorkerConnection(std::unique_ptr<UnixSocket> &socket, const std::shared_ptr<Node> &node);
	virtual ~WorkerConnection();

	State get_state() const;

	void process_request(uint64_t client_id, uint8_t command, const BaseRequest &request);
	void raster_cached();
	void send_hit( const CacheRef &cr );
	void send_partial_hit( const PuzzleRequest &pr );
	void send_miss();
	void release();

	uint64_t get_client_id() const;
	const STRasterRefKeyed& get_new_raster_entry() const;
	const BaseRequest& get_raster_query() const;

	const DeliveryResponse &get_result() const;
	const std::string &get_error_message() const;

	const std::shared_ptr<Node> node;

protected:
	virtual void process_command( uint8_t cmd );

private:
	void reset();
	void process_raster_request(const BaseRequest &req);

	State state;
	uint64_t client_id;
	std::unique_ptr<DeliveryResponse> result;
	std::unique_ptr<STRasterRefKeyed> new_raster_entry;
	std::unique_ptr<BaseRequest> raster_query;
	std::string error_msg;
};

class ControlConnection: public BaseConnection {
public:
	enum class State {
		IDLE
	};
	static const uint32_t MAGIC_NUMBER = 0x42345678;

	//
	// Response from index-server after successful
	// registration of a new node. Data on stream is:
	// id:uint32_t -- the id assigned to the node
	static const uint8_t RESP_HELLO = 50;

	ControlConnection(std::unique_ptr<UnixSocket> &socket, const std::shared_ptr<Node> &node);
	virtual ~ControlConnection();
	const std::shared_ptr<Node> node;

protected:
	virtual void process_command( uint8_t cmd );

private:
	State state;
};

////////////////////////////////////////////////////
//
// Worker
//
////////////////////////////////////////////////////

class DeliveryConnection: public BaseConnection {
public:
	enum class State {
		IDLE, DELIVERY_REQUEST_READ, RASTER_CACHE_REQUEST_READ
	};

	static const uint32_t MAGIC_NUMBER = 0x52345678;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// delivery_id:uint64_t
	//
	static const uint8_t CMD_GET = 60;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// key:STCacheKey
	//
	static const uint8_t CMD_GET_CACHED_RASTER = 61;

	//
	// Response if delivery is send. Data:
	// GenericRaster
	//
	static const uint8_t RESP_OK = 79;

	//
	// Response if delivery faild. Data:
	// message:string -- a description of the error
	//
	static const uint8_t RESP_ERROR = 80;

	DeliveryConnection(std::unique_ptr<UnixSocket> &socket);
	virtual ~DeliveryConnection();

	State get_state() const;

	const STCacheKey& get_key() const;

	uint64_t get_delivery_id() const;

	void send_raster( GenericRaster &raster );

	void send_error( const std::string &msg );

protected:
	virtual void process_command( uint8_t cmd );

private:
	State state;
	uint64_t delivery_id;
	STCacheKey cache_key;
};

#endif /* CONNECTION_H_ */
