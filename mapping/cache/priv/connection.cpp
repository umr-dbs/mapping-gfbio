/*
 * connection.cpp
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#include "cache/priv/connection.h"
#include "cache/index/indexserver.h"
#include "raster/exceptions.h"
#include "util/log.h"
#include "util/make_unique.h"
#include "util/concat.h"

BaseConnection::BaseConnection(std::unique_ptr<UnixSocket>& socket) :
	id(next_id++), stream(*socket), faulty(false), socket(std::move(socket)) {
}

BaseConnection::~BaseConnection() {
}

int BaseConnection::get_read_fd() {
	return socket->getReadFD();
}

void BaseConnection::input() {
	uint8_t cmd;
	try {
		if (stream.read(&cmd, true)) {
			process_command(cmd);
		}
		else {
			Log::debug("Connection closed %d.", id);
			faulty = true;
		}
	} catch (std::exception &e) {
		Log::error("Unexpected error on connection %d. Setting faulty.", id);
		faulty = true;
	}
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

ClientConnection::ClientConnection(std::unique_ptr<UnixSocket>& socket) :
	BaseConnection(socket), state(State::IDLE), request_type(RequestType::NONE) {
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
			std::ostringstream ss;
			ss << "Unknown command on client connection: " << cmd;
			throw NetworkException(ss.str());
		}
	}
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
		} catch (NetworkException &ne) {
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
		} catch (NetworkException &ne) {
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
	request.release();
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

WorkerConnection::WorkerConnection(std::unique_ptr<UnixSocket> &socket, const std::shared_ptr<Node> &node) :
	BaseConnection(socket), node(node), state(State::IDLE) {
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
			STCacheKey key(stream);
			STRasterEntryBounds cube(stream);
			Log::debug("Worker returned new result to raster-cache, key: %s:%d", key.semantic_id.c_str(),
				key.entry_id);
			new_raster_entry.reset(new STRasterRefKeyed(node->id, key, cube));
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
		} catch (NetworkException &ne) {
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
		} catch (NetworkException &ne) {
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
		} catch (NetworkException &ne) {
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
		} catch (NetworkException &ne) {
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

const STRasterRefKeyed& WorkerConnection::get_new_raster_entry() const {
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
	result.release();
	new_raster_entry.release();
	raster_query.release();
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

ControlConnection::ControlConnection(std::unique_ptr<UnixSocket>& socket, const std::shared_ptr<Node> &node) :
	BaseConnection(socket), node(node), state(State::IDLE) {
	try {
		stream.write(RESP_HELLO);
		stream.write(node->id);
	} catch (NetworkException &ne) {
		Log::error("Could not send response to control-connection.");
		faulty = true;
	}
}

ControlConnection::~ControlConnection() {
}

void ControlConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE)
		throw IllegalStateException("Can only accept input in IDLE-state.");

	switch (cmd) {
		default: {
			std::ostringstream msg;
			msg << "Received illegal command on control-connection for node: " << node->id;
			throw NetworkException(msg.str());
		}
	}
}

const uint32_t ControlConnection::MAGIC_NUMBER;
const uint8_t ControlConnection::RESP_HELLO;

/////////////////////////////////////////////////
//
// DELIVERY-CONNECTION
//
/////////////////////////////////////////////////

DeliveryConnection::DeliveryConnection(std::unique_ptr<UnixSocket>& socket) :
	BaseConnection(socket), state(State::IDLE), delivery_id(0), cache_key("", 0) {
}

DeliveryConnection::~DeliveryConnection() {
}

void DeliveryConnection::process_command(uint8_t cmd) {
	if (state != State::IDLE)
		throw IllegalStateException("Can only read from socket in state IDLE");

	switch (cmd) {
		case CMD_GET: {
			stream.read(&delivery_id);
			state = State::DELIVERY_REQUEST_READ;
			break;
		}
		case CMD_GET_CACHED_RASTER: {
			cache_key = STCacheKey(stream);
			state = State::RASTER_CACHE_REQUEST_READ;
			break;
		}
		default:
			// Unknown command
			throw NetworkException(concat("Unknown command on delivery connection: ", cmd));
	}
}

DeliveryConnection::State DeliveryConnection::get_state() const {
	return state;
}

const STCacheKey& DeliveryConnection::get_key() const {
	if (state == State::RASTER_CACHE_REQUEST_READ)
		return cache_key;
	throw IllegalStateException("Can only return cache-key if in state RASTER_CACHE_REQUEST_READ");
}

uint64_t DeliveryConnection::get_delivery_id() const {
	if (state == State::DELIVERY_REQUEST_READ)
		return delivery_id;
	throw IllegalStateException("Can only return cache-key if in state DELIVERY_REQUEST_READ");
}

void DeliveryConnection::send_raster(GenericRaster& raster) {
	if (state == State::RASTER_CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ) {
		try {
			stream.write(RESP_OK);
			raster.toStream(stream);
			state = State::IDLE;
		} catch (NetworkException &ne) {
			Log::error("Could not send response to client.");
			faulty = true;
		}
	}
	else
		throw IllegalStateException(
			"Can only send raster in state DELIVERY_REQUEST_READ or RASTER_CACHE_REQUEST_READ");
}

void DeliveryConnection::send_error(const std::string& msg) {
	if (state == State::RASTER_CACHE_REQUEST_READ || state == State::DELIVERY_REQUEST_READ) {
		try {
			stream.write(RESP_ERROR);
			stream.write(msg);
			state = State::IDLE;
		} catch (NetworkException &ne) {
			Log::error("Could not send response to client.");
			faulty = true;
		}
	}
	else
		throw IllegalStateException(
			"Can only send error in state DELIVERY_REQUEST_READ or RASTER_CACHE_REQUEST_READ");
}

const uint32_t DeliveryConnection::MAGIC_NUMBER;
const uint8_t DeliveryConnection::CMD_GET;
const uint8_t DeliveryConnection::CMD_GET_CACHED_RASTER;
const uint8_t DeliveryConnection::RESP_OK;
const uint8_t DeliveryConnection::RESP_ERROR;
