/*
 * connection.h
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "cache/priv/requests.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/redistribution.h"
#include "util/binarystream.h"

#include "util/concat.h"
#include "util/exceptions.h"

#include <map>
#include <memory>
#include <cstring> //strerror
#include <poll.h>

/**
 * Models a simple blocking connection
 */
class BlockingConnection {
public:
	BlockingConnection() = delete;
	BlockingConnection( const BlockingConnection& ) = delete;
	BlockingConnection( BlockingConnection&& ) = delete;
	BlockingConnection& operator=( const BlockingConnection& ) = delete;
	BlockingConnection& operator=( BlockingConnection&& ) = delete;
	virtual ~BlockingConnection() = default;


	/**
	 * Creates a new blocking connection and immediately sends the given data (e.g. handshake).
	 * @param host the hostname to connect to
	 * @param port the port to connect to
	 * @param no_delay whether to disable nagle's algorithm
	 * @param params the data to send
	 */
	template<typename... Params>
	static std::unique_ptr<BlockingConnection> create(const std::string &host, int port, bool no_delay, const Params &... params );

	/**
	 * Constructs a new instance by opening a socket to the given host and port
	 * @param host the hostname to connect to
	 * @param port the port to connect to
	 * @param no_delay whether to disable nagle's algorithm
	 *
	 */
	BlockingConnection( const std::string host, int port, bool no_delay = true ) :
		socket(BinaryStream::connectTCP(host.c_str(),port,no_delay)) {}

	/**
	 * Writes the given parameters to the underlying stream
	 * @param params the data to write
	 */
	template<typename... Params>
	void write(const Params &... params);

	/**
	 * Reads data from the underlying stream
	 * @return a buffer containing the data read
	 */
	std::unique_ptr<BinaryReadBuffer> read();

	/**
	 * Issues a write followed by a read
	 * @param params the data to write
	 * @return the data read as response to the written data
	 */
	template<typename... Params>
	std::unique_ptr<BinaryReadBuffer> write_and_read(const Params &... params) {
		write(params...);
		return read();
	}

	int get_read_fd() const { return socket.getReadFD(); };
	int get_write_fd() const { return socket.getWriteFD(); };

private:
	template<typename Head>
	void _internal_write(BinaryWriteBuffer &buffer, const Head &head);

	template<typename Head, typename... Tail>
	void _internal_write(BinaryWriteBuffer &buffer, const Head &head, const Tail &... tail);
protected:
	BinaryStream socket;
};

template<typename... Params>
std::unique_ptr<BlockingConnection> BlockingConnection::create(const std::string &host, int port, bool no_delay, const Params &... params ) {
	auto result = make_unique<BlockingConnection>(host,port,no_delay);
	result->write(params...);
	return result;
}

template<typename... Params>
void BlockingConnection::write(const Params &... params) {
	BinaryWriteBuffer buffer;
	_internal_write(buffer, params...);
	socket.write(buffer);
}

template<typename Head>
void BlockingConnection::_internal_write(BinaryWriteBuffer &buffer, const Head &head) {
	buffer.write(head);
}

template<typename Head, typename... Tail>
void BlockingConnection::_internal_write(BinaryWriteBuffer &buffer, const Head &head, const Tail &... tail) {
	buffer.write(head);
	_internal_write(buffer, tail...);
}


class WakeableBlockingConnection : public BlockingConnection {
public:
	/**
	 * Creates a new blocking connection and immediately sends the given data (e.g. handshake).
	 * @param host the hostname to connect to
	 * @param port the port to connect to
	 * @param no_delay whether to disable nagle's algorithm
	 * @param params the data to send
	 */
	template<typename... Params>
	static std::unique_ptr<WakeableBlockingConnection> create(const std::string host, int port, BinaryStream &wakeup_pipe, bool consume_wakeup, bool no_delay, const Params &... params ) {
		auto result = make_unique<WakeableBlockingConnection>(host,port,wakeup_pipe,consume_wakeup,no_delay);
		result->write(params...);
		return result;
	}

	/**
	 * Constructs a new instance by opening a socket to the given host and port
	 * @param host the hostname to connect to
	 * @param port the port to connect to
	 * @param no_delay whether to disable nagle's algorithm
	 *
	 */
	WakeableBlockingConnection( const std::string host, int port, BinaryStream &wakeup_pipe, bool consume_wakeup = false, bool no_delay = true ) :
		BlockingConnection(host,port,no_delay), wakeup_pipe(wakeup_pipe), consume_wakeup(consume_wakeup) {}

	/**
	 * Reads data from the underlying stream, if any.
	 * Blocks until either data is available or the given timeout is reached.
	 * @param timeout the timeout in seconds
	 * @return a buffer containing the data read
	 */
	std::unique_ptr<BinaryReadBuffer> read_timeout(int timeout);
private:
	BinaryStream &wakeup_pipe;
	bool consume_wakeup;
};

class PollableConnection {
public:
	PollableConnection() = delete;
	PollableConnection( const PollableConnection& ) = delete;
	PollableConnection( PollableConnection&& ) = delete;
	PollableConnection& operator=( const PollableConnection& ) = delete;
	PollableConnection& operator=( PollableConnection&& ) = delete;
	virtual ~PollableConnection() = default;

	PollableConnection( BinaryStream &&socket );

	virtual bool is_faulty() const = 0;

	virtual void prepare(struct pollfd *poll_fd) = 0;

	virtual bool process() = 0;
protected:
	std::string flags_to_string( short flags ) const;

	BinaryStream socket;
	struct pollfd *poll_fd;
};

/**
 * Models a newly established non-blocking connection
 */
class NewNBConnection : public PollableConnection {
public:
	NewNBConnection() = delete;
	NewNBConnection( const NewNBConnection& ) = delete;
	NewNBConnection( NewNBConnection&& ) = delete;
	NewNBConnection& operator=( const NewNBConnection& ) = delete;
	NewNBConnection& operator=( NewNBConnection&& ) = delete;

	/**
	 * Constructs a new instance.
	 * The socket is set to non-blocking,
	 * @param remote_addr the address info
	 * @param fd the file-descriptor
	 */
	NewNBConnection( struct sockaddr_storage *remote_addr, int fd );

	/**
	 * Prepares the pollfd structure for the next cycle
	 */
	virtual void prepare(struct pollfd *poll_fd);

	/**
	 * Handles socket-event if any.
	 * @return true if action is required, false otherwise
	 */
	virtual bool process();


	virtual bool is_faulty() const;

	/**
	 * Returns the handshake data read after the connection was established.
	 * Must not be called, before read() returns true.
	 * @return the handshake data
	 */
	BinaryReadBuffer& get_data();

	/**
	 * Releases the underlying stream for usage in concrete connections
	 */
	BinaryStream release_socket();

	std::string hostname;
private:
	bool faulty;
	BinaryReadBuffer buffer;
};

/**
 * Base class for all non-blocking connections
 */
template<typename StateType>
class BaseConnection : public PollableConnection {
private:
	static uint64_t next_id;
public:
	BaseConnection() = delete;
	BaseConnection( const BaseConnection& ) = delete;
	BaseConnection( BaseConnection&& ) = delete;
	BaseConnection& operator=( const BaseConnection& ) = delete;
	BaseConnection& operator=( BaseConnection&& ) = delete;

	/**
	 * Creates a new instance.
	 * @param state the initial stats of the connection
	 * @param socket the underlying socket
	 */
	BaseConnection(StateType state, BinaryStream &&socket);
	virtual ~BaseConnection() = default;

	/**
	 * Prepares the pollfd structure for the next cycle
	 */
	virtual void prepare(struct pollfd *poll_fd);

	/**
	 * Handles socket-event if any.
	 * @return true if action is required, false otherwise
	 */
	virtual bool process();


	/**
	 * @return whether an error occured on this connection and it should be discarded
	 */
	virtual bool is_faulty() const;

	/**
	 * @return the current state of this connection
	 */
	StateType get_state() const;

	time_t get_last_action() const;

	void set_faulty();

	// This connection's id
	const uint64_t id;
protected:
	/**
	 * Callback for commands read from the socket
	 * @param the header-byte read
	 * @param payload the payload data
	 */
	virtual void process_command( uint8_t cmd, BinaryReadBuffer& payload ) = 0;

	/**
	 * Callback invoked whenever a write-request was finished.
	 */
	virtual void write_finished() = 0;

	/**
	 * Called by concrete classes to write data
	 * @param buffer a buffer containing the data to write
	 */
	void begin_write( std::unique_ptr<BinaryWriteBuffer> buffer );

	/**
	 * Changes the connection's state.
	 * @param state the new state
	 */
	void set_state( StateType state );

	/**
	 * Ensures that this connection is in one of the given states
	 */
	template<typename... States>
	void ensure_state( const States... states ) const;

private:
	/**
	 * Called if data is available on the unerlying socket and this connection is not in writing mode
	 * @return whether the data was completely read and action is required
	 */
	bool input();

	/**
	 * Called if data can be written to the unerlying socket and this connection is in writing-mode
	 */
	void output();

	/**
	 * @return whether this connection is currently writing data
	 */
	bool is_writing() const;

	/**
	 * Recursive part of the state check
	 * @param state a valid state
	 * @param states the list of remaining valid states
	 */
	template<typename... States>
	bool _ensure_state( StateType state, States... states) const;

	/**
	 * Recursive part of the state check
	 * @param state a valid state
	 */
	bool _ensure_state( StateType state ) const;

	StateType state;
	bool faulty;
	std::unique_ptr<BinaryReadBuffer> reader;
	std::unique_ptr<BinaryWriteBuffer> writer;
	time_t last_action;
};

/**
 * Models states of ClientConnections
 */
enum class ClientState {
	IDLE, AWAIT_RESPONSE, WRITING_RESPONSE,
	AWAIT_STATS, WRITING_STATS,
	AWAIT_RESET, WRITING_RST
};

/**
 * Models connections to the client-stub
 */
class ClientConnection: public BaseConnection<ClientState> {
public:


	static const uint32_t MAGIC_NUMBER = 0x22345678;

	//
	// Expected data on stream is:
	// BaseRequest
	//
	static const uint8_t CMD_GET = 1;

	static const uint8_t CMD_GET_STATS = 2;
	static const uint8_t CMD_RESET_STATS = 3;

	//
	// Response from index-server after successfully
	// processing a request. Data on stream is:
	// DeliveryResponse
	static const uint8_t RESP_OK = 10;

	static const uint8_t RESP_STATS = 11;

	static const uint8_t RESP_RESETTED = 12;

	//
	// Returned on errors by the index-server.
	// Data on stream is:
	// message:string -- a description of the error
	static const uint8_t RESP_ERROR = 19;

	ClientConnection(BinaryStream &&socket);

	/**
	 * Sends the given response and resets the state to IDLE
	 * @param response the response to send
	 */
	void send_response(const DeliveryResponse &response);

	void send_stats( const SystemStats &stats );

	void confirm_reset();

	/**
	 * Sends the given error and resets the state to IDLE
	 * @param message a description of the error
	 */
	void send_error(const std::string &message);

	/**
	 * Returns the request currently processed. May only
	 * be called in stats AWAIT_RESPONSE.
	 * @return the active request
	 */
	const BaseRequest& get_request() const;

protected:
	void process_command( uint8_t cmd, BinaryReadBuffer& payload );
	void write_finished();
private:
	std::unique_ptr<BaseRequest> request;
};


/**
 * Models states of WorkerConnections
 */
enum class WorkerState {
	IDLE,
	SENDING_REQUEST, PROCESSING, NEW_ENTRY,
	QUERY_REQUESTED, SENDING_QUERY_RESPONSE,
	DONE,
	SENDING_DELIVERY_QTY, WAITING_DELIVERY, DELIVERY_READY,
	ERROR
};

/**
 * Models the connection to a worker-thread
 */
class WorkerConnection: public BaseConnection<WorkerState> {
public:

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

	WorkerConnection(BinaryStream &&socket, uint32_t node_id);

	/**
	 * Sends the given request to the connected worker-thread.
	 * Required state ist IDLE.
	 * @param command the command to use
	 * @param request the request-description
	 */
	void process_request(uint8_t command, const BaseRequest &request);

	/**
	 * Invoked whenever the worker submits a new cache-entry
	 * and it is successfully cached in the global index.
	 * Required state ist NEW_ENTRY.
	 */
	void entry_cached();

	/**
	 * Response to a cache-query issued by the connected worker.
	 * Tells about a full single hit.
	 * Required state ist QUERY_REQUESTED
	 * @param cr the reference to the cache-entry
	 */
	void send_hit( const CacheRef &cr );

	/**
	 * Response to a cache-query issued by the connected worker.
	 * Tells about a partial hit.
	 * Required state ist QUERY_REQUESTED
	 * @param pr the result-description
	 */
	void send_partial_hit( const PuzzleRequest &pr );

	/**
	 * Response to a cache-query issued by the connected worker.
	 * Tells about a full miss.
	 * Required state ist QUERY_REQUESTED.
	 */
	void send_miss();

	/**
	 * Let's the worker known about how many times a result
	 * should be delivered.
	 * Required state is DONE
	 * @param qty the number of times the result should be delivered
	 */
	void send_delivery_qty(uint32_t qty);

	/**
	 * Releases this connection and returns it into IDLE-State.
	 * Required states are DELIVERY_READY, ERROR.
	 */
	void release();

	/**
	 * Retrieves the description of a cache-entry to be placed
	 * in the global index.
	 * Required state is NEW_ENTRY
	 * @return the description of the new cache-entry.
	 */
	const MetaCacheEntry& get_new_entry() const;

	/**
	 * Retrieves the description of a cache-query issued
	 * by the worker.
	 * Required state is QUERY_REQUESTED
	 * @return the query-description
	 */
	const BaseRequest& get_query() const;

	/**
	 * Required state is DELIVERY_READY
	 * @return the id of the result
	 */
	uint64_t get_delivery_id() const;

	/**
	 * Required state is ERROR
	 * @return the error-message sent by the worker.
	 */
	const std::string &get_error_message() const;

	const uint32_t node_id;

protected:
	void process_command( uint8_t cmd, BinaryReadBuffer &payload );
	void write_finished();
private:
	void reset();
	uint64_t delivery_id;
	std::unique_ptr<MetaCacheEntry> new_entry;
	std::unique_ptr<BaseRequest> query;
	std::string error_msg;
};



/**
 * Models states of Control-Connections
 */
enum class ControlState {
	SENDING_HELLO, IDLE,
	SENDING_REORG, REORGANIZING,
	READING_MOVE_RESULT, MOVE_RESULT_READ,
	REORG_FINISHED,
	SENDING_STATS_REQUEST, STATS_REQUESTED, READING_STATS, STATS_RECEIVED
};

/**
 * Models the control-connection to a node
 */
class ControlConnection: public BaseConnection<ControlState> {
public:
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
	// Response from index-server after successful
	// registration of a new node. Data on stream is:
	// id:uint32_t -- the id assigned to the node
	static const uint8_t CMD_HELLO = 44;

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

	static const uint8_t RESP_REORG_REMOVE_REQUEST = 54;

	ControlConnection(BinaryStream &&socket, uint32_t node_id, const std::string &hostname);

	/**
	 * Tells the node to reorganize.
	 * Required state is IDLE
	 * @param desc the description if the actions to take
	 */
	void send_reorg( const ReorgDescription &desc );

	/**
	 * Tells the node about the successfuly migration of an entry in the global index.
	 * Required state is MOVE_RESULT_READ
	 */
	void confirm_move();

	/**
	 * Tells the node to send fresh statistics
	 * Required state is IDLE
	 */
	void send_get_stats();

	/**
	 * Releases this connection back to IDLE
	 * Required states are REORG_FINISHED, STATS_RECEIVED
	 */
	void release();

	/**
	 * Required state is MOVE_RESULT_READ.
	 * @return the description of the migrated cache-entry
	 */
	const ReorgMoveResult& get_move_result() const;

	/**
	 * Required state is STATS_RECEIVED
	 * @return the stats delivered by the node
	 */
	const NodeStats& get_stats() const;

	const uint32_t node_id;
protected:
	virtual void process_command( uint8_t cmd, BinaryReadBuffer &payload );
	virtual void write_finished();
private:
	void reset();
	std::unique_ptr<ReorgMoveResult> move_result;
	std::unique_ptr<NodeStats> stats;
};

////////////////////////////////////////////////////
//
// Worker
//
////////////////////////////////////////////////////

/**
 * Models states of Control-Connections
 */
enum class DeliveryState {
	IDLE,
	DELIVERY_REQUEST_READ,
	CACHE_REQUEST_READ,
	MOVE_REQUEST_READ,
	AWAITING_MOVE_CONFIRM, MOVE_DONE,
	SENDING, SENDING_MOVE, SENDING_CACHE_ENTRY, SENDING_ERROR
};

/**
 * Models a connection to the delivery component.
 */
class DeliveryConnection: public BaseConnection<DeliveryState> {
public:
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

	DeliveryConnection(BinaryStream &&socket);

	/**
	 * Required states are CACHE_REQUEST_READ, MOVE_REQUEST_READ, AWAITING_MOVE_CONFIRM, MOVE_DONE
	 * @return the key of the cache-entry to deliver
	 */
	const TypedNodeCacheKey& get_key() const;

	/**
	 * Required state is DELIVERY_REQUEST_READ
	 * @return the id of the delivery to send
	 */
	uint64_t get_delivery_id() const;

	/**
	 * Sends the given data-item.
	 * Required state is DELIVERY_REQUEST_READ
	 * @param item the data-item to send
	 */
	template <typename T>
	void send( std::shared_ptr<const T> item );

	/**
	 * Sends the given data-item prepended with the given fetch-info
	 * Required state is CACHE_REQUEST_READ
	 * @param info information about size and costs of the entry
	 * @param item the data-item to send
	 */
	template <typename T>
	void send_cache_entry( const FetchInfo &info, std::shared_ptr<const T> item );

	/**
	 * Sends the given data-item prepended with all meta-information of the corresponding cache-entry
	 * Required state is MOVE_REQUEST_READ
	 * @param info the cache-meta-information
	 * @param item the data-item to send
	 */
	template <typename T>
	void send_move( const CacheEntry &info, std::shared_ptr<const T> item );

	/**
	 * Sends the given error-message
	 * Required states are CACHE_REQUEST_READ, DELIVERY_REQUEST_READ, MOVE_REQUEST_READ
	 * @param msg the message to send
	 */
	void send_error( const std::string &msg );

	/**
	 * Releases this connection back to IDLE.
	 * Required state is MOVE_DONE.
	 */
	void finish_move();

protected:
	void process_command( uint8_t cmd, BinaryReadBuffer &payload );
	void write_finished();
private:
	template<typename T>
	void write_data( BinaryWriteBuffer &buffer, std::shared_ptr<const T> &item );

	uint64_t delivery_id;
	TypedNodeCacheKey cache_key;
};

enum class ClientDeliveryState {
	REQUEST_SENT
};

class NBClientDeliveryConnection : public BaseConnection<ClientDeliveryState> {
public:
	static std::unique_ptr<NBClientDeliveryConnection> create( const DeliveryResponse &dr );
	NBClientDeliveryConnection(BinaryStream &&stream);
protected:
	void process_command( uint8_t cmd, BinaryReadBuffer& payload );
	void write_finished();
};

#endif /* CONNECTION_H_ */
