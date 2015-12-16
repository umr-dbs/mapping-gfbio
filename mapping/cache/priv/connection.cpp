/*
 * connection.cpp
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#include "cache/priv/connection.h"
#include "cache/index/indexserver.h"
#include "util/exceptions.h"
#include "util/log.h"
#include "util/make_unique.h"
#include "util/concat.h"

#include <fcntl.h>

BaseConnection::BaseConnection(std::unique_ptr<UnixSocket> socket) :
	id(next_id++), writing(false), reading(false), faulty(false), stream(*socket), socket(std::move(socket)) {
	int flags = fcntl(get_read_fd(), F_GETFL, 0);
	fcntl(get_read_fd(), F_SETFL, flags | O_NONBLOCK);

	flags = fcntl(get_write_fd(), F_GETFL, 0);
	fcntl(get_write_fd(), F_SETFL, flags | O_NONBLOCK);
}

BaseConnection::~BaseConnection() {
}

void BaseConnection::input() {
	// If we are processing a non-blocking read
	if (reading) {
		reader->read(socket->getReadFD());
		if (reader->is_finished()) {
			Log::trace("Finished reading on connection: %d, read %d bytes", id, reader->get_total_read());
			reading = false;
			read_finished(*reader);
			reader.reset();
		}
		else if (reader->has_error()) {
			Log::warn("An error occured during read on connection: %d", id);
			reading = false;
			faulty = true;
			reader.reset();
		}
		else
			Log::trace("Read-buffer full. Continuing on next call on connection: %d", id);
	}
	// If we are expecting commands
	else {
		uint8_t cmd;
		try {
			if (stream.read(&cmd, true))
				process_command(cmd);
			else {
				Log::debug("Connection closed %d.", id);
				faulty = true;
			}
		} catch (const std::exception &e) {
			Log::error("Unexpected error on connection %d, setting faulty. Reason: %s", id, e.what());
			faulty = true;
		}
	}
}

void BaseConnection::output() {
	if (writing) {
		writer->write(socket->getWriteFD());
		if (writer->is_finished()) {
			writing = false;
			write_finished();
			writer.reset(nullptr);
		}
		else if (writer->has_error()) {
			Log::warn("An error occured during write on connection: %d", id);
			writing = false;
			faulty = true;
			writer.reset(nullptr);
		}
		else
			Log::trace("Write-buffer full. Continuing on next call.");
	}
	else
		throw IllegalStateException("Cannot trigger write while not in writing state.");
}

void BaseConnection::begin_write(std::unique_ptr<NBWriter> writer) {
	if (!writing && !reading) {
		this->writer = std::move(writer);
		this->writing = true;
//		output();
	}
	else
		throw IllegalStateException("Cannot start nb-write. Another read or write action is in progress.");
}

void BaseConnection::begin_read(std::unique_ptr<NBReader> reader) {
	if (!writing && !reading) {
		this->reader = std::move(reader);
		this->reading = true;
//		input();
	}
	else
		throw IllegalStateException("Cannot start nb-read. Another read or write action is in progress.");
}

int BaseConnection::get_read_fd() {
	return socket->getReadFD();
}

int BaseConnection::get_write_fd() {
	return socket->getWriteFD();
}

bool BaseConnection::is_reading() {
	return reading;
}

bool BaseConnection::is_writing() {
	return writing;
}

bool BaseConnection::is_faulty() {
	return faulty;
}

uint64_t BaseConnection::next_id = 1;

/////////////////////////////////////////////////
//
// CLIENT-CONNECTION
//
/////////////////////////////////////////////////

ClientConnection::ClientConnection(std::unique_ptr<UnixSocket> socket) :
	BaseConnection(std::move(socket)), state(State::IDLE) {
}

ClientConnection::~ClientConnection() {
}

void ClientConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE)
		throw IllegalStateException("Can only accept input in state IDLE");

	switch (cmd) {
		case CMD_GET:
			state = State::READING_REQUEST;
			Log::debug("Reading request from client");
			begin_read(make_unique<NBBaseRequestReader>());
			break;
		default:
			throw NetworkException(concat("Unknown command on client connection: ", cmd));
	}
}

void ClientConnection::read_finished(NBReader& reader) {
	switch (state) {
		case State::READING_REQUEST:
			request.reset(new BaseRequest(*reader.get_stream()));
			state = State::AWAIT_RESPONSE;
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ClientConnection");
	}
}

void ClientConnection::write_finished() {
	switch (state) {
		case State::WRITING_RESPONSE:
			reset();
			break;
		default:
			throw IllegalStateException("Unexpected end of writing in ClientConnection");
	}
}

ClientConnection::State ClientConnection::get_state() const {
	return state;
}

void ClientConnection::send_response(const DeliveryResponse& response) {
	if (state == State::AWAIT_RESPONSE) {
		state = State::WRITING_RESPONSE;
		begin_write(
			make_unique<NBMessageWriter<DeliveryResponse>>(RESP_OK, response)
		);
	}
	else
		throw IllegalStateException("Can only send response in state: AWAIT_RESPONSE");
}

void ClientConnection::send_error(const std::string& message) {
	if (state == State::AWAIT_RESPONSE) {
		state = State::WRITING_RESPONSE;
		begin_write(make_unique<NBMessageWriter<std::string>>(RESP_ERROR, message));
	}
	else
		throw IllegalStateException("Can only send error in state: AWAIT_RESPONSE");
}

const BaseRequest& ClientConnection::get_request() const {
	if (state == State::AWAIT_RESPONSE)
		return *request;
	throw IllegalStateException("Can only return request in state AWAIT_RESPONSE and type != UNKNOWN");
}

void ClientConnection::reset() {
	request.reset();
	state = State::IDLE;
}

const uint32_t ClientConnection::MAGIC_NUMBER;
const uint8_t ClientConnection::CMD_GET;
const uint8_t ClientConnection::RESP_OK;
const uint8_t ClientConnection::RESP_ERROR;

/////////////////////////////////////////////////
//
// WORKER-CONNECTION
//
/////////////////////////////////////////////////

WorkerConnection::WorkerConnection(std::unique_ptr<UnixSocket> socket, const std::shared_ptr<Node> &node) :
	BaseConnection(std::move(socket)), node(node), state(State::IDLE) {
}

WorkerConnection::~WorkerConnection() {
}

WorkerConnection::State WorkerConnection::get_state() const {
	return state;
}

void WorkerConnection::process_command(uint8_t cmd) {
	if (state != State::PROCESSING && state != State::WAITING_DELIVERY)
		throw IllegalStateException("Can only accept input in state PROCESSING or WAITING_DELIVERY.");

	switch (cmd) {
		case RESP_RESULT_READY: {
			// Read delivery id
			Log::debug("Worker finished processing. Determinig delivery qty.");
			state = State::DONE;
			break;
		}
		case RESP_DELIVERY_READY: {
			state = State::READING_DELIVERY_ID;
			Log::debug("Worker created delivery. Done");
			begin_read( make_unique<NBFixedSizeReader>( sizeof(uint64_t) ) );
			break;
		}
		case CMD_QUERY_CACHE: {
			state = State::READING_QUERY;
			Log::debug("Worker requested raster cache query.");
			begin_read( make_unique<NBBaseRequestReader>() );
			break;
		}
		case RESP_NEW_CACHE_ENTRY: {
			state = State::READING_ENTRY;
			Log::debug("Worker returned new result to raster-cache");
			begin_read( make_unique<NBNodeCacheRefReader>() );
			break;
		}
		case RESP_ERROR: {
			state = State::READING_ERROR;
			begin_read( make_unique<NBStringReader>() );
			break;
		}
		default: {
			Log::error("Worker returned unknown code: %d. Terminating worker-connection.", cmd);
			throw NetworkException(concat("Unknown response from worker: ", cmd));
		}
	}
}

void WorkerConnection::read_finished(NBReader& reader) {
	switch (state) {
		case State::READING_DELIVERY_ID: {
			uint64_t delivery_id;
			reader.get_stream()->read(&delivery_id);
			result.reset(new DeliveryResponse(node->host, node->port, delivery_id));
			state = State::DELIVERY_READY;
			break;
		}
		case State::READING_QUERY:
			query.reset(new BaseRequest(*reader.get_stream()));
			state = State::QUERY_REQUESTED;
			break;
		case State::READING_ENTRY:
			new_entry.reset(new NodeCacheRef(*reader.get_stream()));
			state = State::NEW_ENTRY;
			break;
		case State::READING_ERROR:
			reader.get_stream()->read(&error_msg);
			Log::warn("Worker returned error: %s", error_msg.c_str());
			state = State::ERROR;
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in WorkerConnection");
	}
}

void WorkerConnection::write_finished() {
	switch (state) {
		case State::SENDING_REQUEST:
		case State::SENDING_QUERY_RESPONSE:
			state = State::PROCESSING;
			break;
		case State::SENDING_DELIVERY_QTY:
			state = State::WAITING_DELIVERY;
			break;
		default:
			throw IllegalStateException("Unexpected end of writing in WorkerConnection");
	}
}

// ACTIONS

void WorkerConnection::process_request(uint8_t command, const BaseRequest& request) {
	if (state == State::IDLE) {
		state = State::SENDING_REQUEST;
		begin_write(
			make_unique<NBMessageWriter<BaseRequest>>(command, request, true));
	}
	else
		throw IllegalStateException("Can only process requests when idle");
}

void WorkerConnection::entry_cached() {
	if (state == State::NEW_ENTRY) {
		// TODO: Do we need a confirmation of this
		state = State::PROCESSING;
	}
	else
		throw IllegalStateException("Can only ack new entry in state NEW_ENTRY");
}

void WorkerConnection::send_hit(const CacheRef& cr) {
	if (state == State::QUERY_REQUESTED) {
		state = State::SENDING_QUERY_RESPONSE;
		begin_write(
			make_unique<NBMessageWriter<CacheRef>>(RESP_QUERY_HIT, cr));
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_partial_hit(const PuzzleRequest& pr) {
	if (state == State::QUERY_REQUESTED) {
		state = State::SENDING_QUERY_RESPONSE;
		begin_write(
			make_unique<NBMessageWriter<PuzzleRequest>>(RESP_QUERY_PARTIAL, pr));
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_miss() {
	if (state == State::QUERY_REQUESTED) {
		state = State::SENDING_QUERY_RESPONSE;
		begin_write(make_unique<NBSimpleWriter<uint8_t>>(RESP_QUERY_MISS));
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_delivery_qty(uint32_t qty) {
	if (state == State::DONE) {
		state = State::SENDING_DELIVERY_QTY;
		begin_write(
			make_unique<NBMessageWriter<uint32_t>>(RESP_DELIVERY_QTY, qty));
	}
	else
		throw IllegalStateException("Can only send delivery qty in state DONE");

}

void WorkerConnection::release() {
	if (state == State::DELIVERY_READY || state == State::ERROR)
		reset();
	else
		throw IllegalStateException("Can only release worker in state DONE or ERROR");
}

//
// GETTER
//

const NodeCacheRef& WorkerConnection::get_new_entry() const {
	if (state == State::NEW_ENTRY)
		return *new_entry;
	throw IllegalStateException("Can only return new raster entry in state NEW_RASTER_ENTRY");
}

const BaseRequest& WorkerConnection::get_query() const {
	if (state == State::QUERY_REQUESTED)
		return *query;
	throw IllegalStateException("Can only return raster query in state RASTER_QUERY_REQUESTED");
}

const DeliveryResponse& WorkerConnection::get_result() const {
	if (state == State::DELIVERY_READY)
		return *result;
	throw IllegalStateException("Can only return result in state DELIVERY_READY");
}

const std::string& WorkerConnection::get_error_message() const {
	if (state == State::ERROR)
		return error_msg;
	throw IllegalStateException("Can only return error-message in state ERROR");
}

void WorkerConnection::reset() {
	error_msg = "";
	result.reset();
	new_entry.reset();
	query.reset();
	state = State::IDLE;
}

const uint32_t WorkerConnection::MAGIC_NUMBER;
const uint8_t WorkerConnection::CMD_CREATE;
const uint8_t WorkerConnection::CMD_DELIVER;
const uint8_t WorkerConnection::CMD_PUZZLE;
const uint8_t WorkerConnection::RESP_RESULT_READY;
const uint8_t WorkerConnection::RESP_DELIVERY_READY;
const uint8_t WorkerConnection::RESP_NEW_CACHE_ENTRY;
const uint8_t WorkerConnection::CMD_QUERY_CACHE;
const uint8_t WorkerConnection::RESP_ERROR;
const uint8_t WorkerConnection::RESP_QUERY_HIT;
const uint8_t WorkerConnection::RESP_QUERY_MISS;
const uint8_t WorkerConnection::RESP_QUERY_PARTIAL;
const uint8_t WorkerConnection::RESP_DELIVERY_QTY;

/////////////////////////////////////////////////
//
// CONTROL-CONNECTION
//
/////////////////////////////////////////////////

ControlConnection::ControlConnection(std::unique_ptr<UnixSocket> socket, const std::string &hostname) :
	BaseConnection(std::move(socket)), hostname(hostname), state(State::READING_HANDSHAKE) {
	begin_read( make_unique<NBNodeHandshakeReader>() );
}

ControlConnection::~ControlConnection() {
}

ControlConnection::State ControlConnection::get_state() const {
	return state;
}

void ControlConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE && state != State::REORGANIZING && state != State::STATS_REQUESTED)
		throw IllegalStateException("Can only accept input in state IDLE, REORGANIZING or STATS_REQUESTED");

	switch (cmd) {
		case RESP_REORG_ITEM_MOVED: {
			state = State::READING_REORG_RESULT;
			Log::debug("Reading ReorgResult.");
			begin_read(make_unique<NBReorgMoveResultReader>());
			break;
		}
		case RESP_REORG_DONE: {
			state = State::REORG_FINISHED;
			break;
		}
		case RESP_STATS: {
			state = State::READING_STATS;
			Log::debug("Reading NodeStats.");
			begin_read(make_unique<NBNodeStatsReader>());
			break;
		}
		default: {
			throw NetworkException(
				concat("Received illegal command on control-connection for node: ", node->id));
		}
	}
}

void ControlConnection::read_finished(NBReader& reader) {
	switch (state) {
		case State::READING_REORG_RESULT:
			reorg_result.reset(new ReorgMoveResult(*reader.get_stream()));
			state = State::REORG_RESULT_READ;
			break;
		case State::READING_STATS:
			stats.reset(new NodeStats(*reader.get_stream()));
			state = State::STATS_RECEIVED;
			break;
		case State::READING_HANDSHAKE:
			handshake.reset( new NodeHandshake(*reader.get_stream()));
			state = State::HANDSHAKE_READ;
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ControlConnection");
	}
}

void ControlConnection::write_finished() {
	switch (state) {
		case State::SENDING_REORG:
			state = State::REORGANIZING;
			break;
		case State::SENDING_REORG_CONFIRM:
			state = State::REORGANIZING;
			break;
		case State::SENDING_STATS_REQUEST:
			state = State::STATS_REQUESTED;
			break;
		case State::SENDING_HELLO:
			state = State::IDLE;
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ControlConnection");
	}
}

// ACTIONS

void ControlConnection::confirm_handshake(std::shared_ptr<Node> node) {
	if ( state == State::HANDSHAKE_READ ) {
		this->node = node;
		state = State::SENDING_HELLO;
		begin_write(
			make_unique<NBHelloWriter>( node->id, hostname )
		);
	}
	else
		throw IllegalStateException("Can only confirm handshake in state HANDSHAKE_READ");
}

void ControlConnection::send_reorg(const ReorgDescription& desc) {
	if (state == State::IDLE) {
		state = State::SENDING_REORG;
		begin_write(
			make_unique<NBMessageWriter<ReorgDescription>>(CMD_REORG, desc));
	}
	else
		throw IllegalStateException("Can only trigger reorg in state IDLE");
}

void ControlConnection::confirm_reorg() {
	if (state == State::REORG_RESULT_READ) {
		state = State::SENDING_REORG_CONFIRM;
		begin_write(make_unique<NBSimpleWriter<uint8_t>>(CMD_REORG_ITEM_OK));
	}
	else
		throw IllegalStateException("Can only send raster query result in state REORG_RESULT_READ");

}

void ControlConnection::send_get_stats() {
	if (state == State::IDLE) {
		state = State::SENDING_STATS_REQUEST;
		begin_write(make_unique<NBSimpleWriter<uint8_t>>(CMD_GET_STATS));
	}
	else
		throw IllegalStateException("Can only request statistics in state IDLE");
}

void ControlConnection::release() {
	if (state == State::REORG_FINISHED || state == State::STATS_RECEIVED)
		reset();
	else
		throw IllegalStateException(
			"Can only release control-connection in state REORG_FINISHED, STATS_RECEIVED or ERROR");
}

//
// GETTER
//

const NodeHandshake& ControlConnection::get_handshake() {
	if ( state == State::HANDSHAKE_READ )
		return *handshake;
	else
		throw IllegalStateException("Can only return Handshake in state HANDSHAKE_READ");
}

const ReorgMoveResult& ControlConnection::get_result() {
	if (state == State::REORG_RESULT_READ)
		return *reorg_result;
	else
		throw IllegalStateException("Can only return ReorgResult in state REORG_RESULT_READ");
}

const NodeStats& ControlConnection::get_stats() {
	if (state == State::STATS_RECEIVED)
		return *stats;
	else
		throw IllegalStateException("Can only return ReorgResult in state REORG_RESULT_READ");
}

// Private stuff

void ControlConnection::reset() {
	reorg_result.reset();
	stats.reset();
	state = State::IDLE;
}

const uint32_t ControlConnection::MAGIC_NUMBER;
const uint8_t ControlConnection::CMD_REORG;
const uint8_t ControlConnection::CMD_GET_STATS;
const uint8_t ControlConnection::CMD_REORG_ITEM_OK;
const uint8_t ControlConnection::CMD_HELLO;
const uint8_t ControlConnection::RESP_REORG_ITEM_MOVED;
const uint8_t ControlConnection::RESP_REORG_DONE;
const uint8_t ControlConnection::RESP_STATS;

/////////////////////////////////////////////////
//
// DELIVERY-CONNECTION
//
/////////////////////////////////////////////////

DeliveryConnection::DeliveryConnection(std::unique_ptr<UnixSocket> socket) :
	BaseConnection(std::move(socket)), state(State::IDLE), delivery_id(0), cache_key(CacheType::UNKNOWN,"", 0) {
}

DeliveryConnection::~DeliveryConnection() {
}

void DeliveryConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE && state != State::AWAITING_MOVE_CONFIRM)
		throw IllegalStateException("Can only read from socket in state IDLE and AWAITING_MOVE_CONFIRM");

	switch (cmd) {
		case CMD_GET: {
			state = State::READING_DELIVERY_REQUEST;
			Log::debug("Reading delivery id");
			begin_read(make_unique<NBFixedSizeReader>(sizeof(delivery_id)));
			break;
		}
		case CMD_GET_CACHED_ITEM: {
			state = State::READING_CACHE_REQUEST;
			Log::debug("Reading TypedNodeCacheKey for direct delivery.");
			begin_read(make_unique<NBTypedNodeCacheKeyReader>());
			break;
		}
		case CMD_MOVE_ITEM: {
			state = State::READING_MOVE_REQUEST;
			Log::debug("Reading TypedNodeCacheKey for move delivery.");
			begin_read(make_unique<NBTypedNodeCacheKeyReader>());
			break;
		}
		case CMD_MOVE_DONE: {
			state = State::MOVE_DONE;
			break;
		}
		default:
			// Unknown command
			throw NetworkException(concat("Unknown command on delivery connection: ", cmd));
	}
}

void DeliveryConnection::read_finished(NBReader& reader) {
	switch (state) {
		case State::READING_DELIVERY_REQUEST: {
			reader.get_stream()->read(&delivery_id);
			state = State::DELIVERY_REQUEST_READ;
			break;
		}
		case State::READING_CACHE_REQUEST: {
			cache_key = TypedNodeCacheKey(*reader.get_stream());
			state = State::CACHE_REQUEST_READ;
			break;
		}
		case State::READING_MOVE_REQUEST: {
			cache_key = TypedNodeCacheKey(*reader.get_stream());
			state = State::MOVE_REQUEST_READ;
			break;
		}
		default:
			throw IllegalStateException("Unexpected end of reading in DeliveryConnection");
	}
}

void DeliveryConnection::write_finished() {
	switch (state) {
		case State::SENDING:
		case State::SENDING_CACHE_ENTRY: {
			state = State::IDLE;
			break;
		}
		case State::SENDING_MOVE: {
			state = State::AWAITING_MOVE_CONFIRM;
			break;
		}
		case State::SENDING_ERROR: {
			state = State::IDLE;
			break;
		}
		default:
			throw IllegalStateException("Unexpected end of writing in DeliveryConnection");
	}
}

DeliveryConnection::State DeliveryConnection::get_state() const {
	return state;
}

const TypedNodeCacheKey& DeliveryConnection::get_key() const {
	if (state == State::CACHE_REQUEST_READ || state == State::MOVE_REQUEST_READ
		|| state == State::AWAITING_MOVE_CONFIRM || state == State::MOVE_DONE)
		return cache_key;
	throw IllegalStateException("Can only return cache-key if in state RASTER_CACHE_REQUEST_READ");
}

uint64_t DeliveryConnection::get_delivery_id() const {
	if (state == State::DELIVERY_REQUEST_READ)
		return delivery_id;
	throw IllegalStateException("Can only return cache-key if in state DELIVERY_REQUEST_READ");
}

template<typename T>
void DeliveryConnection::send(std::shared_ptr<const T> item) {
	if (state == State::CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ) {
		state = State::SENDING;
		begin_write (
			make_unique<NBMultiWriter>(
					make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
					get_data_writer(item)
			)
		);
	}
	else
		throw IllegalStateException(
			"Can only send item in state DELIVERY_REQUEST_READ or CACHE_REQUEST_READ");
}

template<typename T>
void DeliveryConnection::send_cache_entry(const MoveInfo& info,
		std::shared_ptr<const T> item) {

	if (state == State::CACHE_REQUEST_READ) {
		state = State::SENDING_CACHE_ENTRY;
		begin_write(
			make_unique<NBMultiWriter>(
				make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
				make_unique<NBSimpleWriter<MoveInfo>>(info),
				get_data_writer(item)
			)
		);
	}
	else
		throw IllegalStateException("Can only move item in state CACHE_REQUEST_READ");
}

template<typename T>
void DeliveryConnection::send_move(const CacheEntry& info,
		std::shared_ptr<const T> item) {

	if (state == State::MOVE_REQUEST_READ) {
		state = State::SENDING_MOVE;
		begin_write(
			make_unique<NBMultiWriter>(
				make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
				make_unique<NBSimpleWriter<CacheEntry>>(info),
				get_data_writer(item)
			)
		);
	}
	else
		throw IllegalStateException("Can only move item in state MOVE_REQUEST_READ");
}


void DeliveryConnection::send_error(const std::string& msg) {
	if (state == State::CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ
		|| state == State::MOVE_REQUEST_READ || state == State::SENDING
		|| state == State::SENDING_MOVE) {

		state = State::SENDING_ERROR;
		begin_write(make_unique<NBMessageWriter<std::string>>(RESP_ERROR, msg));
	}
	else
		throw IllegalStateException(
			"Can only send error in state DELIVERY_REQUEST_READ or RASTER_CACHE_REQUEST_READ");
}

void DeliveryConnection::release() {
	if (state == State::MOVE_DONE)
		state = State::IDLE;
	else
		throw IllegalStateException("Can only release connection in state MOVE_DONE");
}

template<typename T>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const T> item) {
	throw ArgumentException("No writer present for given type");
}

template<>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const PointCollection> item) {
	return make_unique<NBPointsWriter>( item );
}

template<>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const LineCollection> item) {
	return make_unique<NBLinesWriter>( item );
}

template<>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const PolygonCollection> item) {
	return make_unique<NBPolygonsWriter>( item );
}

template<>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const GenericPlot> item) {
	return make_unique<NBPlotWriter>( item );
}

template<>
std::unique_ptr<NBWriter> DeliveryConnection::get_data_writer(
		std::shared_ptr<const GenericRaster> item) {
	return make_unique<NBRasterWriter>( item );
}

const uint32_t DeliveryConnection::MAGIC_NUMBER;
const uint8_t DeliveryConnection::CMD_GET;
const uint8_t DeliveryConnection::CMD_GET_CACHED_ITEM;
const uint8_t DeliveryConnection::CMD_MOVE_ITEM;
const uint8_t DeliveryConnection::CMD_MOVE_DONE;
const uint8_t DeliveryConnection::RESP_OK;
const uint8_t DeliveryConnection::RESP_ERROR;

template void DeliveryConnection::send(std::shared_ptr<const GenericRaster>);
template void DeliveryConnection::send(std::shared_ptr<const PointCollection>);
template void DeliveryConnection::send(std::shared_ptr<const LineCollection>);
template void DeliveryConnection::send(std::shared_ptr<const PolygonCollection>);
template void DeliveryConnection::send(std::shared_ptr<const GenericPlot>);

template void DeliveryConnection::send_cache_entry( const MoveInfo&, std::shared_ptr<const GenericRaster> );
template void DeliveryConnection::send_cache_entry( const MoveInfo&, std::shared_ptr<const PointCollection> );
template void DeliveryConnection::send_cache_entry( const MoveInfo&, std::shared_ptr<const LineCollection> );
template void DeliveryConnection::send_cache_entry( const MoveInfo&, std::shared_ptr<const PolygonCollection> );
template void DeliveryConnection::send_cache_entry( const MoveInfo&, std::shared_ptr<const GenericPlot> );

template void DeliveryConnection::send_move( const CacheEntry&, std::shared_ptr<const GenericRaster> );
template void DeliveryConnection::send_move( const CacheEntry&, std::shared_ptr<const PointCollection> );
template void DeliveryConnection::send_move( const CacheEntry&, std::shared_ptr<const LineCollection> );
template void DeliveryConnection::send_move( const CacheEntry&, std::shared_ptr<const PolygonCollection> );
template void DeliveryConnection::send_move( const CacheEntry&, std::shared_ptr<const GenericPlot> );

