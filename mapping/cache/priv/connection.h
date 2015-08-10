/*
 * connection.h
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "util/binarystream.h"
#include "cache/node/node_cache.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"

#include <map>
#include <memory>

class Node;

class BaseConnection {
public:
	BaseConnection(std::unique_ptr<UnixSocket> socket);
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

	ClientConnection(std::unique_ptr<UnixSocket> socket);
	virtual ~ClientConnection();

	State get_state() const;

	// Sends the given response and resets the state to IDLE
	void send_response(const DeliveryResponse &response);
	// Sends the given error and resets the state to IDLE
	void send_error(const std::string &message);

	RequestType get_request_type() const;
	const BaseRequest& get_request() const;

protected:
	virtual void process_command( uint8_t cmd );

private:
	void reset();
	State state;
	RequestType request_type;
	std::unique_ptr<BaseRequest> request;
};

class WorkerConnection: public BaseConnection {
public:
	enum class State {
		IDLE, PROCESSING, NEW_RASTER_ENTRY, RASTER_QUERY_REQUESTED, DONE, WAITING_DELIVERY, DELIVERY_READY, ERROR
	};
	static const uint32_t MAGIC_NUMBER = 0x32345678;

	//
	// Expected data on stream is:
	// request:BaseRequest
	//
	static const uint8_t CMD_CREATE_RASTER = 20;

	//
	// Expected data on stream is:
	// request:DeliveryRequest
	//
	static const uint8_t CMD_DELIVER_RASTER = 21;

	//
	// Expected data on stream is:
	// request:PuzzleRequest
	//
	static const uint8_t CMD_PUZZLE_RASTER = 22;

	//
	// Expected data on stream is:
	// BaseRequest
	//
	static const uint8_t CMD_QUERY_RASTER_CACHE = 23;

	//
	// Response from worker to signal finished computation
	//
	static const uint8_t RESP_RESULT_READY = 30;

	//
	// Response from worker to signal ready to deliver result
	// Data on stream is:
	// DeliveryResponse
	//
	static const uint8_t RESP_DELIVERY_READY = 31;

	//
	// Send if a new raster-entry is added to the local cache
	// Data on stream is:
	// key:STCacheKey
	// cube:RasterCacheCube
	//
	static const uint8_t RESP_NEW_RASTER_CACHE_ENTRY = 32;

	//
	// Response from index-server after successfully
	// probing the cache for a RESP_QUERY_RASTER_CACHE.
	// Data on stream is:
	// ref:CacheRef
	static const uint8_t RESP_QUERY_HIT = 33;

	//
	// Response from index-server after unsuccessfuly
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Theres no data on stream.
	static const uint8_t RESP_QUERY_MISS = 34;

	//
	// Response from index-server after successfully
	// probing the cache for a CMD_INDEX_QUERY_CACHE.
	// Data on stream is:
	// puzzle-request: PuzzleRequest
	static const uint8_t RESP_QUERY_PARTIAL = 36;

	//
	// Response from index to tell delivery qty
	// Data on stream:
	// qty:uint32_t
	//
	static const uint8_t RESP_DELIVERY_QTY = 37;

	//
	// Send if a worker cannot fulfill the request
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_ERROR = 39;

	WorkerConnection(std::unique_ptr<UnixSocket> socket, const std::shared_ptr<Node> &node);
	virtual ~WorkerConnection();

	State get_state() const;

	void process_request(uint8_t command, const BaseRequest &request);
	void raster_cached();
	void send_hit( const CacheRef &cr );
	void send_partial_hit( const PuzzleRequest &pr );
	void send_miss();
	void send_delivery_qty(uint32_t qty);
	void release();

	const NodeCacheRef& get_new_raster_entry() const;
	const BaseRequest& get_raster_query() const;

	const DeliveryResponse &get_result() const;
	const std::string &get_error_message() const;

	const std::shared_ptr<Node> node;

protected:
	virtual void process_command( uint8_t cmd );

private:
	void reset();

	State state;
	std::unique_ptr<DeliveryResponse> result;
	std::unique_ptr<NodeCacheRef> new_raster_entry;
	std::unique_ptr<BaseRequest> raster_query;
	std::string error_msg;
};

class ControlConnection: public BaseConnection {
public:
	enum class State {
		IDLE, REORGANIZING, REORG_RESULT_READ, REORG_FINISHED
	};
	static const uint32_t MAGIC_NUMBER = 0x42345678;

	//
	// Tells the index that the node finished
	// reorganization of a single item.
	// Data on stream is:
	// ReorgResult
	//
	static const uint8_t CMD_REORG_ITEM_MOVED = 40;

	//
	// Response from worker to signal that the reorganization
	// is finished.
	//
	static const uint8_t CMD_REORG_DONE = 41;

	//
	// Response from index-server after successful
	// registration of a new node. Data on stream is:
	// id:uint32_t -- the id assigned to the node
	static const uint8_t RESP_HELLO = 50;

	//
	// Tells the node to fetch the attached
	// items and store them in its local cache
	// ReorgDescription
	//
	static const uint8_t RESP_REORG = 51;

	//
	// Tells the node that the reorg on index was OK
	// There is no data on stream
	//
	static const uint8_t RESP_REORG_ITEM_OK = 52;

	State get_state() const;

	void send_reorg( const ReorgDescription &desc );
	void confirm_reorg();
	void release();

	const ReorgResult& get_result();

	ControlConnection(std::unique_ptr<UnixSocket> socket, const std::shared_ptr<Node> &node);
	virtual ~ControlConnection();
	const std::shared_ptr<Node> node;

protected:
	virtual void process_command( uint8_t cmd );

private:
	void reset();
	State state;
	std::unique_ptr<ReorgResult> reorg_result;
};

////////////////////////////////////////////////////
//
// Worker
//
////////////////////////////////////////////////////

class DeliveryConnection: public BaseConnection {
public:
	enum class State {
		IDLE, DELIVERY_REQUEST_READ, RASTER_CACHE_REQUEST_READ, RASTER_MOVE_REQUEST_READ, AWAITING_MOVE_CONFIRM, MOVE_DONE
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
	// Command to pick up a delivery.
	// Expected data on stream is:
	// key:STCacheKey
	//
	static const uint8_t CMD_MOVE_RASTER = 62;

	//
	// Command to pick up a delivery.
	// No expected data on stream.
	//
	static const uint8_t CMD_MOVE_DONE = 63;


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

	DeliveryConnection(std::unique_ptr<UnixSocket> socket);
	virtual ~DeliveryConnection();

	State get_state() const;

	const NodeCacheKey& get_key() const;

	uint64_t get_delivery_id() const;

	void send_raster( GenericRaster &raster );

	void send_raster_move( GenericRaster &raster );

	void send_error( const std::string &msg );

	void release();

protected:
	virtual void process_command( uint8_t cmd );

private:
	State state;
	uint64_t delivery_id;
	NodeCacheKey cache_key;
};

#endif /* CONNECTION_H_ */
