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
	id(next_id++), stream(*socket), faulty(false), writing(false), reading(false), socket(std::move(socket)) {
}

BaseConnection::~BaseConnection() {
}

void BaseConnection::input() {
	// If we are processing a non-blocking read
	if (reading) {
		reader->read();
		if (reader->is_finished()) {
			Log::debug("Finished reading on connection: %d", id);
			reading = false;
			read_finished(*reader);
			reader.reset();
		}
		else if (writer->has_error()) {
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
		writer->write();
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
	}
	else
		throw IllegalStateException("Cannot start nb-write. Another read or write action is in progress.");
}

void BaseConnection::begin_read(std::unique_ptr<NBReader> reader) {
	if (!writing && !reading) {
		this->reader = std::move(reader);
		this->reading = true;
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
	BaseConnection(std::move(socket)), state(State::IDLE), request_type(RequestType::NONE) {
}

ClientConnection::~ClientConnection() {
}

void ClientConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE)
		throw IllegalStateException("Can only accept input in state IDLE");

	switch (cmd) {
		case CMD_GET_RASTER: {
			request.reset(new BaseRequest(stream));
			request_type = RequestType::RASTER;
			state = State::AWAIT_RESPONSE;
			break;
		}
			// More to come
		default: {
			throw NetworkException( concat("Unknown command on client connection: ", cmd) );
		}
	}
}

void ClientConnection::read_finished(NBReader& reader) {
	(void) reader;
}

void ClientConnection::write_finished() {
}

ClientConnection::State ClientConnection::get_state() const {
	return state;
}

void ClientConnection::send_response(const DeliveryResponse& response) {
	if (state == State::AWAIT_RESPONSE) {
		try {
			stream.write(RESP_OK);
			response.toStream(stream);
			reset();
		} catch (const NetworkException &ne) {
			Log::error("Could not send response to client.");
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send response in state: AWAIT_RESPONSE");
}

void ClientConnection::send_error(const std::string& message) {
	if (state == State::AWAIT_RESPONSE) {
		try {
			stream.write(RESP_ERROR);
			stream.write(message);
			reset();
		} catch (const NetworkException &ne) {
			Log::error("Could not send response to client.");
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send error in state: AWAIT_RESPONSE");
}

ClientConnection::RequestType ClientConnection::get_request_type() const {
	if (state == State::AWAIT_RESPONSE)
		return request_type;
	throw IllegalStateException("Can only tell type in state AWAIT_RESPONSE");
}

const BaseRequest& ClientConnection::get_request() const {
	if (state == State::AWAIT_RESPONSE && request_type == RequestType::RASTER)
		return *request;
	throw IllegalStateException("Can only return raster_request in state AWAIT_RESPONSE and type was RASTER");
}

void ClientConnection::reset() {
	request.reset();
	request_type = RequestType::NONE;
	state = State::IDLE;
}

const uint32_t ClientConnection::MAGIC_NUMBER;
const uint8_t ClientConnection::CMD_GET_RASTER;
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
			Log::debug("Finished processing raster-request from client.");
			break;
		}
		case RESP_DELIVERY_READY: {
			Log::debug("Worker created delivery. Done");
			uint64_t delivery_id;
			stream.read(&delivery_id);
			result.reset(new DeliveryResponse(node->host, node->port, delivery_id));
			state = State::DELIVERY_READY;
			break;
		}
		case CMD_QUERY_RASTER_CACHE: {
			Log::debug("Worker requested raster cache query.");
			raster_query.reset(new BaseRequest(stream));
			state = State::RASTER_QUERY_REQUESTED;
			Log::debug("Finished processing raster-request from worker.");
			break;
		}
		case RESP_NEW_RASTER_CACHE_ENTRY: {
			new_raster_entry.reset(new NodeCacheRef(stream));
			Log::debug("Worker returned new result to raster-cache: %s",
				new_raster_entry->to_string().c_str());
			state = State::NEW_RASTER_ENTRY;
			break;
		}
		case RESP_ERROR: {
			stream.read(&error_msg);
			Log::warn("Worker returned error: %s", error_msg.c_str());
			state = State::ERROR;
			break;
		}
		default: {
			Log::error("Worker returned unknown code: %d. Terminating worker-connection.", cmd);
			throw NetworkException(concat("Unknown response from worker: ", cmd));
		}
	}
}

void WorkerConnection::read_finished(NBReader& reader) {
	(void) reader;
}

void WorkerConnection::write_finished() {
}

// ACTIONS

void WorkerConnection::process_request(uint8_t command, const BaseRequest& request) {
	if (state == State::IDLE) {
		state = State::PROCESSING;
		stream.write(command);
		request.toStream(stream);
	}
	else
		throw IllegalStateException("Can only process requests when idle");
}

void WorkerConnection::raster_cached() {
	if (state == State::NEW_RASTER_ENTRY) {
		// TODO: Do we need a confirmation of this
		state = State::PROCESSING;
	}
	else
		throw IllegalStateException("Can only ack new raster entry in state NEW_RASTER_ENTRY");
}

void WorkerConnection::send_hit(const CacheRef& cr) {
	if (state == State::RASTER_QUERY_REQUESTED) {
		try {
			stream.write(RESP_QUERY_HIT);
			cr.toStream(stream);
			state = State::PROCESSING;
		} catch (const NetworkException &ne) {
			Log::error("Could not send HIT-response to worker: %d", id);
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_partial_hit(const PuzzleRequest& pr) {
	if (state == State::RASTER_QUERY_REQUESTED) {
		try {
			stream.write(RESP_QUERY_PARTIAL);
			pr.toStream(stream);
			state = State::PROCESSING;
		} catch (const NetworkException &ne) {
			Log::error("Could not send PARTIAL-HIT-response to worker: %d", id);
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_miss() {
	if (state == State::RASTER_QUERY_REQUESTED) {
		try {
			stream.write(RESP_QUERY_MISS);
			state = State::PROCESSING;
		} catch (const NetworkException &ne) {
			Log::error("Could not send MISS-response to worker: %d", id);
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send raster query result in state RASTER_QUERY_REQUESTED");
}

void WorkerConnection::send_delivery_qty(uint32_t qty) {
	if (state == State::DONE) {
		try {
			stream.write(RESP_DELIVERY_QTY);
			stream.write(qty);
			state = State::WAITING_DELIVERY;
		} catch (const NetworkException &ne) {
			Log::error("Could not send MISS-response to worker: %d", id);
			faulty = true;
		}
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

const NodeCacheRef& WorkerConnection::get_new_raster_entry() const {
	if (state == State::NEW_RASTER_ENTRY)
		return *new_raster_entry;
	throw IllegalStateException("Can only return new raster entry in state NEW_RASTER_ENTRY");
}

const BaseRequest& WorkerConnection::get_raster_query() const {
	if (state == State::RASTER_QUERY_REQUESTED)
		return *raster_query;
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
	new_raster_entry.reset();
	raster_query.reset();
	state = State::IDLE;
}

const uint32_t WorkerConnection::MAGIC_NUMBER;
const uint8_t WorkerConnection::CMD_CREATE_RASTER;
const uint8_t WorkerConnection::CMD_DELIVER_RASTER;
const uint8_t WorkerConnection::CMD_PUZZLE_RASTER;
const uint8_t WorkerConnection::RESP_RESULT_READY;
const uint8_t WorkerConnection::RESP_DELIVERY_READY;
const uint8_t WorkerConnection::RESP_NEW_RASTER_CACHE_ENTRY;
const uint8_t WorkerConnection::CMD_QUERY_RASTER_CACHE;
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

ControlConnection::ControlConnection(std::unique_ptr<UnixSocket> socket, const std::shared_ptr<Node> &node) :
	BaseConnection(std::move(socket)), node(node), state(State::IDLE) {
	try {
		stream.write(CMD_HELLO);
		stream.write(node->id);
	} catch (const NetworkException &ne) {
		Log::error("Could not send response to control-connection.");
		faulty = true;
	}
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
			reorg_result.reset(new ReorgMoveResult(stream));
			state = State::REORG_RESULT_READ;
			break;
		}
		case RESP_REORG_DONE: {
			state = State::REORG_FINISHED;
			break;
		}
		case RESP_STATS: {
			stats.reset(new NodeStats(stream));
			state = State::STATS_RECEIVED;
			break;
		}
		default: {
			throw NetworkException(
				concat("Received illegal command on control-connection for node: ", node->id));
		}
	}
}


void ControlConnection::read_finished(NBReader& reader) {
	(void) reader;
}

void ControlConnection::write_finished() {
}

// ACTIONS

void ControlConnection::send_reorg(const ReorgDescription& desc) {
	if (state == State::IDLE) {
		try {
			stream.write(CMD_REORG);
			desc.toStream(stream);
			state = State::REORGANIZING;
		} catch (const NetworkException &ne) {
			Log::error("Could not send Reorg-request to node: %d", node->id);
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only trigger reorg in state IDLE");
}

void ControlConnection::confirm_reorg() {
	if (state == State::REORG_RESULT_READ) {
		try {
			stream.write(CMD_REORG_ITEM_OK);
			state = State::REORGANIZING;
		} catch (const NetworkException &ne) {
			Log::error("Could not send MISS-response to worker: %d", id);
			faulty = true;
		}
	}
	else
		throw IllegalStateException("Can only send raster query result in state REORG_RESULT_READ");

}

void ControlConnection::send_get_stats() {
	if (state == State::IDLE) {
		try {
			stream.write(CMD_GET_STATS);
			state = State::STATS_REQUESTED;
		} catch (const NetworkException &ne) {
			Log::error("Could not send stats-request to node: %d", node->id);
			faulty = true;
		}
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
	BaseConnection(std::move(socket)), state(State::IDLE), delivery_id(0), cache_key("", 0) {
}

DeliveryConnection::~DeliveryConnection() {
}

void DeliveryConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE && state != State::AWAITING_MOVE_CONFIRM)
		throw IllegalStateException("Can only read from socket in state IDLE and AWAITING_MOVE_CONFIRM");

	switch (cmd) {
		case CMD_GET: {
			state = State::READING_DELIVERY_REQUEST;
			begin_read(make_unique<FixedSizeReader>(get_read_fd(), sizeof(delivery_id)));
			break;
		}
		case CMD_GET_CACHED_RASTER: {
			state = State::READING_RASTER_CACHE_REQUEST;
			begin_read(make_unique<NodeCacheKeyReader>(get_read_fd()));
			break;
		}
		case CMD_MOVE_RASTER: {
			state = State::READING_RASTER_MOVE_REQUEST;
			begin_read(make_unique<NodeCacheKeyReader>(get_read_fd()));
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
		case State::READING_RASTER_CACHE_REQUEST: {
			cache_key = NodeCacheKey(*reader.get_stream());
			state = State::RASTER_CACHE_REQUEST_READ;
			break;
		}
		case State::READING_RASTER_MOVE_REQUEST: {
			cache_key = NodeCacheKey(*reader.get_stream());
			state = State::RASTER_MOVE_REQUEST_READ;
			break;
		}
		default:
			throw IllegalStateException("Unexpected end of reading in DeliveryConnection");
	}
}

void DeliveryConnection::write_finished() {
	switch (state) {
		case State::SENDING_RASTER: {
			state = State::IDLE;
			break;
		}

		case State::SENDING_RASTER_MOVE: {
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

DeliveryConnection::State
DeliveryConnection::get_state()
const {
	return state;
}

const NodeCacheKey& DeliveryConnection::get_key() const {
	if (state == State::RASTER_CACHE_REQUEST_READ ||
		state == State::RASTER_MOVE_REQUEST_READ ||
		state == State::AWAITING_MOVE_CONFIRM ||
		state == State::MOVE_DONE)
	return cache_key;
	throw IllegalStateException("Can only return cache-key if in state RASTER_CACHE_REQUEST_READ");
}

uint64_t
DeliveryConnection::get_delivery_id()
const {
	if (state == State::DELIVERY_REQUEST_READ)
	return delivery_id;
	throw IllegalStateException("Can only return cache-key if in state DELIVERY_REQUEST_READ");
}

void DeliveryConnection::send_raster(std::shared_ptr<GenericRaster> raster) {
	if (state == State::RASTER_CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ) {
		state = State::SENDING_RASTER;
		begin_write( make_unique<NBMessageWriter>(RESP_OK,
				make_unique<NBRasterWriter>( raster, get_write_fd() ),
				get_write_fd() ) );
	}
	else
	throw IllegalStateException(
		"Can only send raster in state DELIVERY_REQUEST_READ or RASTER_CACHE_REQUEST_READ");
}

void DeliveryConnection::send_error(const std::string& msg) {
	if (state == State::RASTER_CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ ||
		state == State::RASTER_MOVE_REQUEST_READ || state == State::SENDING_RASTER ||
		state == State::SENDING_RASTER_MOVE ) {

		state = State::SENDING_ERROR;
		begin_write( make_unique<NBErrorWriter>(RESP_ERROR,msg,get_write_fd()) );
	}
	else
	throw IllegalStateException(
		"Can only send error in state DELIVERY_REQUEST_READ or RASTER_CACHE_REQUEST_READ");
}

void DeliveryConnection::send_raster_move(std::shared_ptr<GenericRaster> raster) {
	if ( state == State::RASTER_MOVE_REQUEST_READ ) {
		state = State::SENDING_RASTER_MOVE;
		begin_write( make_unique<NBMessageWriter>(RESP_OK,
				make_unique<NBRasterWriter>( raster, get_write_fd() ),
				get_write_fd() ) );
	}
	else
	throw IllegalStateException("Can only move raster in state RASTER_MOVE_REQUEST_READ");

}

void DeliveryConnection::release() {
	if ( state == State::MOVE_DONE )
	state = State::IDLE;
	else
	throw IllegalStateException("Can only release connection in state MOVE_DONE");
}

const uint32_t DeliveryConnection::MAGIC_NUMBER;
const uint8_t DeliveryConnection::CMD_GET;
const uint8_t DeliveryConnection::CMD_GET_CACHED_RASTER;
const uint8_t DeliveryConnection::CMD_MOVE_RASTER;
const uint8_t DeliveryConnection::CMD_MOVE_DONE;
const uint8_t DeliveryConnection::RESP_OK;
const uint8_t DeliveryConnection::RESP_ERROR;

