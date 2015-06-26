/*
 * connection.cpp
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#include "cache/index/connection.h"
#include "cache/index/indexserver.h"
#include "raster/exceptions.h"
#include "util/log.h"
#include "util/make_unique.h"

IndexConnection::IndexConnection(SP& socket) :
	id(next_id++), stream(*socket), socket(std::move(socket)) {
}

IndexConnection::~IndexConnection() {
}

int IndexConnection::get_read_fd() {
	return socket->getReadFD();
}

uint64_t IndexConnection::next_id = 1;

/////////////////////////////////////////////////
//
// CLIENT-CONNECTION
//
/////////////////////////////////////////////////

ClientConnection::ClientConnection(SP& socket) :
	IndexConnection(socket), state(State::IDLE), request_type(RequestType::NONE) {
}

ClientConnection::~ClientConnection() {
}

void ClientConnection::input() {

	uint8_t cmd;
	stream.read(&cmd);
	switch (cmd) {
		case Common::CMD_INDEX_GET_RASTER: {
			raster_request.reset(new RasterBaseRequest(stream));
			request_type = RequestType::RASTER;
			state = State::REQUEST_READ;
			break;
		}
		// More to come
		default: {
			Log::warn("Unknown command on frontend-connection: %d. Dropping connection.", cmd);
			break;
		}
	}
}

ClientConnection::State ClientConnection::get_state() const {
	return state;
}

void ClientConnection::send_response(const DeliveryResponse& response) {
	if ( state == State::PROCESSING ) {
		uint8_t cmd = Common::RESP_INDEX_GET;
		stream.write(cmd);
		response.toStream(stream);
		reset();
	}
	else
		throw IllegalStateException("Can only send error in processing state");
}

void ClientConnection::send_error(const std::string& message) {
	if ( state == State::PROCESSING || state == State::REQUEST_READ ) {
		uint8_t cmd = Common::RESP_INDEX_ERROR;
		stream.write(cmd);
		stream.write(message);
		reset();
	}
	else
		throw IllegalStateException("Can only send error in processing or request-read state");
}

void ClientConnection::retry() {
	if ( state == State::PROCESSING )
		state = State::REQUEST_READ;
	else
		throw IllegalStateException("Can only go back to REQUEST_READ when in PROCESSING");
}

void ClientConnection::processing() {
	if ( state == State::REQUEST_READ )
		state = State::PROCESSING;
	else
		throw IllegalStateException("Can only go to PROCESSING when in REQUEST_READ");
}

ClientConnection::RequestType ClientConnection::get_request_type() const {
	if ( state == State::REQUEST_READ || state == State::PROCESSING )
		return request_type;
	throw IllegalStateException("Can only tell type if state in [REQUEST_READ,PROCESSING]");
}

const RasterBaseRequest& ClientConnection::get_raster_request() const {
	if ( (state == State::REQUEST_READ || state == State::PROCESSING) && request_type == RequestType::RASTER )
		return *raster_request;
	throw IllegalStateException("Can only return raster_request if state in [REQUEST_READ,PROCESSING] and type was RASTER");
}

void ClientConnection::reset() {
	raster_request.release();
	request_type = RequestType::NONE;
	state = State::IDLE;
}

/////////////////////////////////////////////////
//
// WORKER-CONNECTION
//
/////////////////////////////////////////////////

WorkerConnection::WorkerConnection(SP& socket, uint32_t node_id, RasterRefCache &raster_cache, const std::map<uint32_t,std::shared_ptr<Node>> &nodes) :
	IndexConnection(socket), node(nodes.at(node_id)), raster_cache(raster_cache), nodes(nodes), state(State::IDLE), client_id(-1) {
}

WorkerConnection::~WorkerConnection() {
}

WorkerConnection::State WorkerConnection::get_state() const {
	return state;
}

void WorkerConnection::input() {
	if ( state != State::PROCESSING )
		throw IllegalStateException("Can only accept input in PROCESSING-state.");

	uint8_t resp;
	stream.read(&resp);

	switch (resp) {
		case Common::RESP_WORKER_RESULT_READY: {
			// Read delivery id
			uint64_t delivery_id;
			stream.read(&delivery_id);
			Log::debug("Worker returned result, delivery_id: %d", delivery_id);
			result.reset( new DeliveryResponse(node->host,node->port,delivery_id) );
			state = State::DONE;
			Log::debug("Finished processing raster-request from client.");
			break;
		}
		case Common::CMD_INDEX_QUERY_RASTER_CACHE: {
			Log::debug("Processing raster-request from worker.");
			BaseRequest req(stream);
			process_raster_request(req);
			Log::debug("Finished processing raster-request from worker.");
			break;
		}
		case Common::RESP_WORKER_NEW_RASTER_CACHE_ENTRY: {
			STCacheKey key(stream);
			STRasterEntryBounds cube(stream);
			Log::debug("Worker returned new result to raster-cache, key: %s:%d", key.semantic_id.c_str(), key.entry_id);
			std::unique_ptr<STRasterRef> e = std::make_unique<STRasterRef>(node->id, key.entry_id, cube );
			raster_cache.put(key.semantic_id, e );
			break;
		}
		case Common::RESP_WORKER_ERROR: {
			stream.read(&error_msg);
			Log::warn("Worker returned error: %s", error_msg.c_str());
			state = State::ERROR;
			break;
		}
		default: {
			Log::error("Worker returned unknown code: %d. Terminating worker-connection.", resp);
			throw NetworkException("Unknown response from worker.");
		}
	}

}

void WorkerConnection::process_request( uint64_t client_id, uint8_t command, const BaseRequest& request) {
	if ( state == State::IDLE ) {
		state = State::PROCESSING;
		this->client_id = client_id;
		stream.write(command);
		request.toStream(stream);
	}
	else
		throw IllegalStateException("Can only process requests when idle");
}

const DeliveryResponse& WorkerConnection::get_result() const {
	if ( state == State::DONE )
		return *result;
	throw IllegalStateException("Can only return result in done-state.");
}

const std::string& WorkerConnection::get_error_message() const {
	if ( state == State::ERROR )
		return error_msg;
	throw IllegalStateException("Can only return error-message in error-state.");
}

uint64_t WorkerConnection::get_client_id() const {
	if ( state == State::PROCESSING || state == State:: DONE )
		return client_id;
	throw IllegalStateException("Can only return client_id when processing or done.");
}

void WorkerConnection::reset() {
	client_id = -1;
	error_msg = "";
	result.release();
	state = State::IDLE;
}

void WorkerConnection::process_raster_request(const BaseRequest& req) {
	Log::debug("Querying raster-cache for: %s::%s", req.semantic_id.c_str(), Common::qr_to_string(req.query).c_str());

	STQueryResult res = raster_cache.query( req.semantic_id, req.query );

	Log::debug("QueryResult: %s", res.to_string().c_str() );

	uint8_t resp;

	// Full single hit
	if ( res.ids.size() == 1 && !res.has_remainder() ) {
		Log::debug("Full HIT. Sending reference.");
		auto ref = raster_cache.get( req.semantic_id, res.ids[0] );
		auto node = nodes.at( ref->node_id );
		resp = Common::RESP_INDEX_HIT;
		CacheRef cr(node->host,node->port,ref->cache_id);
		stream.write(resp);
		cr.toStream(stream);
	}
	// Puzzle
	else if ( res.has_hit() && res.coverage > 0.1 ) {
		Log::debug("Partial HIT. Sending puzzle-request, coverage: %f", res.coverage);
		std::vector<CacheRef> entries;
		for ( auto id : res.ids ) {
			auto &ref  = raster_cache.get( req.semantic_id, id );
			auto &node = nodes.at( ref->node_id );
			entries.push_back( CacheRef(node->host,node->port,ref->cache_id) );
		}
		PuzzleRequest pr( req.semantic_id, req.query, res.covered, res.remainder, entries );
		resp = Common::RESP_INDEX_PARTIAL;
		stream.write(resp);
		pr.toStream(stream);
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		resp = Common::RESP_INDEX_MISS;
		stream.write(resp);
	}
}

/////////////////////////////////////////////////
//
// CONTROL-CONNECTION
//
/////////////////////////////////////////////////

ControlConnection::ControlConnection(SP& socket, const std::shared_ptr<Node> &node) :
	IndexConnection(socket), node(node) {
	uint8_t code = Common::RESP_INDEX_NODE_HELLO;
	stream.write(code);
	stream.write(id);

}

ControlConnection::~ControlConnection() {
}

void ControlConnection::input() {
	uint8_t cmd;
	stream.read(&cmd);
	switch (cmd) {
		default: {
			std::ostringstream msg;
			msg << "Received illegal command on control-connection for node: " << node->id;
			throw NetworkException(msg.str());
		}
	}
}
