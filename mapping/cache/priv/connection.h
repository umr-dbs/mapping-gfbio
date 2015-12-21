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
#include "cache/priv/cache_stats.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/common.h"
#include "util/nio.h"

#include <map>
#include <memory>

class Node;

class BaseConnection {
public:
	BaseConnection(std::unique_ptr<BinaryFDStream> socket);
	virtual ~BaseConnection();
	// Called if data is available on the unerlying socket and this connection is not in writing mode
	void input();
	// Called if data can be written to the unerlying socket and this connection is in writing-mode
	void output();

	// Returns the fd used for writes by this connection
	int get_write_fd();
	// Returns the fd used for reads by this connection
	int get_read_fd();
	// Tells whether an error occured on this connection
	bool is_faulty();
	// Tells whether this connection is currently reading data
	bool is_reading();
	// Tells whether this connection is currently writing data
	bool is_writing();

	// This connection's id
	const uint64_t id;
protected:
	// Callback for commands read from the socket
	virtual void process_command( uint8_t cmd ) = 0;
	// Callback for finished non-blocking writes
	virtual void write_finished() = 0;
	// Callback for finished non-blocking reads
	virtual void read_finished( NBReader& reader) = 0;
	// Called by implementing classes to trigger a non-blocking write
	void begin_write( std::unique_ptr<NBWriter> writer );
	// Called by implementing classes to trigger a non-blocking read
	void begin_read( std::unique_ptr<NBReader> reader );
private:
	bool writing;
	bool reading;
	bool faulty;
	BinaryStream &stream;
	std::unique_ptr<NBWriter> writer;
	std::unique_ptr<NBReader> reader;
	std::unique_ptr<BinaryFDStream> socket;
	static uint64_t next_id;
};

class ClientConnection: public BaseConnection {
public:
	enum class State {
		IDLE, READING_REQUEST, AWAIT_RESPONSE, WRITING_RESPONSE
	};

	static const uint32_t MAGIC_NUMBER = 0x22345678;

	//
	// Expected data on stream is:
	// BaseRequest
	//
	static const uint8_t CMD_GET = 1;

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

	ClientConnection(std::unique_ptr<BinaryFDStream> socket);
	virtual ~ClientConnection();

	State get_state() const;

	// Sends the given response and resets the state to IDLE
	void send_response(const DeliveryResponse &response);
	// Sends the given error and resets the state to IDLE
	void send_error(const std::string &message);

	const BaseRequest& get_request() const;

protected:
	virtual void process_command( uint8_t cmd );
	virtual void write_finished();
	virtual void read_finished( NBReader& reader);
private:
	void reset();
	State state;
	std::unique_ptr<BaseRequest> request;
};

class WorkerConnection: public BaseConnection {
public:
	enum class State {
		IDLE,
		SENDING_REQUEST, PROCESSING, READING_ENTRY, NEW_ENTRY,
		READING_QUERY, QUERY_REQUESTED, SENDING_QUERY_RESPONSE,
		DONE,
		SENDING_DELIVERY_QTY, WAITING_DELIVERY, READING_DELIVERY_ID, DELIVERY_READY,
		READING_ERROR, ERROR
	};
	static const uint32_t MAGIC_NUMBER = 0x32345678;

	//
	// Expected data on stream is:
	// request:BaseRequest
	//
	static const uint8_t CMD_CREATE = 20;

	//
	// Expected data on stream is:
	// request:DeliveryRequest
	//
	static const uint8_t CMD_DELIVER = 21;

	//
	// Expected data on stream is:
	// request:PuzzleRequest
	//
	static const uint8_t CMD_PUZZLE = 22;

	//
	// Expected data on stream is:
	// BaseRequest
	//
	static const uint8_t CMD_QUERY_CACHE = 23;

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
	static const uint8_t RESP_NEW_CACHE_ENTRY = 32;

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

	WorkerConnection(std::unique_ptr<BinaryFDStream> socket, const std::shared_ptr<Node> &node);
	virtual ~WorkerConnection();

	State get_state() const;

	void process_request(uint8_t command, const BaseRequest &request);
	void entry_cached();
	void send_hit( const CacheRef &cr );
	void send_partial_hit( const PuzzleRequest &pr );
	void send_miss();
	void send_delivery_qty(uint32_t qty);
	void release();

	const NodeCacheRef& get_new_entry() const;
	const BaseRequest& get_query() const;

	const DeliveryResponse &get_result() const;
	const std::string &get_error_message() const;

	const std::shared_ptr<Node> node;

protected:
	virtual void process_command( uint8_t cmd );
	virtual void write_finished();
	virtual void read_finished( NBReader& reader);
private:
	void reset();

	State state;
	std::unique_ptr<DeliveryResponse> result;
	std::unique_ptr<NodeCacheRef> new_entry;
	std::unique_ptr<BaseRequest> query;
	std::string error_msg;
};

class ControlConnection: public BaseConnection {
public:
	enum class State {
		READING_HANDSHAKE, HANDSHAKE_READ, SENDING_HELLO,
		IDLE,
		SENDING_REORG, REORGANIZING, READING_REORG_RESULT, REORG_RESULT_READ, SENDING_REORG_CONFIRM, REORG_FINISHED,
		SENDING_STATS_REQUEST, STATS_REQUESTED, READING_STATS, STATS_RECEIVED
	};
	static const uint32_t MAGIC_NUMBER = 0x42345678;

	//
	// Tells the node to fetch the attached
	// items and store them in its local cache
	// ReorgDescription
	//
	static const uint8_t CMD_REORG = 40;

	//
	// Tells the node to send an update
	// of the local cache stats
	//
	static const uint8_t CMD_GET_STATS = 41;

	//
	// Tells the node that the reorg on index was OK
	// There is no data on stream
	//
	static const uint8_t CMD_REORG_ITEM_OK = 42;

	//
	// Response from index-server after successful
	// registration of a new node. Data on stream is:
	// id:uint32_t -- the id assigned to the node
	static const uint8_t CMD_HELLO = 43;

	//
	// Tells the index that the node finished
	// reorganization of a single item.
	// Data on stream is:
	// ReorgResult
	//
	static const uint8_t RESP_REORG_ITEM_MOVED = 51;

	//
	// Response from worker to signal that the reorganization
	// is finished.
	//
	static const uint8_t RESP_REORG_DONE = 52;

	//
	// Response from worker including stats update
	//
	static const uint8_t RESP_STATS = 53;

	State get_state() const;

	void confirm_handshake( std::shared_ptr<Node> node );
	void send_reorg( const ReorgDescription &desc );
	void confirm_reorg();
	void send_get_stats();

	void release();

	const NodeHandshake& get_handshake();
	const ReorgMoveResult& get_result();
	const NodeStats& get_stats();


	ControlConnection(std::unique_ptr<BinaryFDStream> socket, const std::string &hostname);
	virtual ~ControlConnection();
	std::shared_ptr<Node> node;
	const std::string hostname;
protected:
	virtual void process_command( uint8_t cmd );
	virtual void write_finished();
	virtual void read_finished( NBReader& reader);
private:
	void reset();
	State state;
	std::unique_ptr<NodeHandshake> handshake;
	std::unique_ptr<ReorgMoveResult> reorg_result;
	std::unique_ptr<NodeStats> stats;
};

////////////////////////////////////////////////////
//
// Worker
//
////////////////////////////////////////////////////

class DeliveryConnection: public BaseConnection {
public:
	enum class State {
		IDLE,
		READING_DELIVERY_REQUEST, DELIVERY_REQUEST_READ,
		READING_CACHE_REQUEST, CACHE_REQUEST_READ,
		READING_MOVE_REQUEST, MOVE_REQUEST_READ,
		AWAITING_MOVE_CONFIRM, MOVE_DONE,
		SENDING, SENDING_MOVE, SENDING_CACHE_ENTRY, SENDING_ERROR
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
	// TypedNodeCacheKey
	//
	static const uint8_t CMD_GET_CACHED_ITEM = 61;

	//
	// Command to pick up a delivery.
	// Expected data on stream is:
	// TypedNodeCacheKey
	//
	static const uint8_t CMD_MOVE_ITEM = 62;

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

	DeliveryConnection(std::unique_ptr<BinaryFDStream> socket);
	virtual ~DeliveryConnection();

	State get_state() const;

	const TypedNodeCacheKey& get_key() const;

	uint64_t get_delivery_id() const;

	template <typename T>
	void send( std::shared_ptr<const T> item );

	template <typename T>
	void send_cache_entry( const MoveInfo &info, std::shared_ptr<const T> item );

	template <typename T>
	void send_move( const CacheEntry &info, std::shared_ptr<const T> item );

	void send_error( const std::string &msg );

	void release();

protected:
	virtual void process_command( uint8_t cmd );
	virtual void write_finished();
	virtual void read_finished( NBReader& reader);
private:
template<typename T>
std::unique_ptr<NBWriter> get_data_writer( std::shared_ptr<const T> item );

	State state;
	uint64_t delivery_id;
	TypedNodeCacheKey cache_key;
};

#endif /* CONNECTION_H_ */
