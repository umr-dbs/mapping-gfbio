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

template<typename StateType>
BaseConnection<StateType>::BaseConnection(StateType state, std::unique_ptr<BinaryFDStream> socket) :
	id(next_id++), state(state), writing(false), reading(false), faulty(false), stream(*socket), socket(std::move(socket)) {
	int flags = fcntl(get_read_fd(), F_GETFL, 0);
	fcntl(get_read_fd(), F_SETFL, flags | O_NONBLOCK);

	flags = fcntl(get_write_fd(), F_GETFL, 0);
	fcntl(get_write_fd(), F_SETFL, flags | O_NONBLOCK);
}

template<typename StateType>
BaseConnection<StateType>::~BaseConnection() {
}

template<typename StateType>
void BaseConnection<StateType>::input() {
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

template<typename StateType>
void BaseConnection<StateType>::output() {
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

template<typename StateType>
void BaseConnection<StateType>::begin_write(std::unique_ptr<NBWriter> writer) {
	if (!writing && !reading) {
		this->writer = std::move(writer);
		this->writing = true;
//		output();
	}
	else
		throw IllegalStateException("Cannot start nb-write. Another read or write action is in progress.");
}

template<typename StateType>
void BaseConnection<StateType>::begin_read(std::unique_ptr<NBReader> reader) {
	if (!writing && !reading) {
		this->reader = std::move(reader);
		this->reading = true;
//		input();
	}
	else
		throw IllegalStateException("Cannot start nb-read. Another read or write action is in progress.");
}

template<typename StateType>
int BaseConnection<StateType>::get_read_fd() const {
	return socket->getReadFD();
}

template<typename StateType>
int BaseConnection<StateType>::get_write_fd() const {
	return socket->getWriteFD();
}

template<typename StateType>
bool BaseConnection<StateType>::is_reading() const {
	return reading;
}

template<typename StateType>
bool BaseConnection<StateType>::is_writing() const {
	return writing;
}

template<typename StateType>
bool BaseConnection<StateType>::is_faulty() const {
	return faulty;
}

template<typename StateType>
StateType BaseConnection<StateType>::get_state() const {
	return state;
}

template<typename StateType>
void BaseConnection<StateType>::set_state(StateType state) {
	this->state = state;
}

template<typename StateType>
template<typename... States>
void BaseConnection<StateType>::ensure_state(const States... states) const {
	if ( !_ensure_state(states...) )
		throw IllegalStateException("Illegal state");
}

template<typename StateType>
template<typename... States>
bool BaseConnection<StateType>::_ensure_state(StateType state,
		States... states) const {
	return this->state == state || _ensure_state(states...);
}

template<typename StateType>
bool BaseConnection<StateType>::_ensure_state(StateType state) const {
	return this->state == state;
}

template<typename StateType>
uint64_t BaseConnection<StateType>::next_id = 1;

/////////////////////////////////////////////////
//
// CLIENT-CONNECTION
//
/////////////////////////////////////////////////

ClientConnection::ClientConnection(std::unique_ptr<BinaryFDStream> socket) :
	BaseConnection(ClientState::IDLE, std::move(socket)) {
}

ClientConnection::~ClientConnection() {
}

void ClientConnection::process_command(uint8_t cmd) {
	ensure_state(ClientState::IDLE);
	switch (cmd) {
		case CMD_GET:
			set_state(ClientState::READING_REQUEST);
			begin_read(make_unique<NBBaseRequestReader>());
			break;
		default:
			throw NetworkException(concat("Unknown command on client connection: ", cmd));
	}
}

void ClientConnection::read_finished(NBReader& reader) {
	switch (get_state()) {
		case ClientState::READING_REQUEST:
			request.reset(new BaseRequest(*reader.get_stream()));
			set_state(ClientState::AWAIT_RESPONSE);
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ClientConnection");
	}
}

void ClientConnection::write_finished() {
	switch (get_state()) {
		case ClientState::WRITING_RESPONSE:
			reset();
			break;
		default:
			throw IllegalStateException("Unexpected end of writing in ClientConnection");
	}
}

void ClientConnection::send_response(const DeliveryResponse& response) {
	ensure_state(ClientState::AWAIT_RESPONSE);
	set_state(ClientState::WRITING_RESPONSE);
	begin_write(
		make_unique<NBMessageWriter<DeliveryResponse>>(RESP_OK, response)
	);
}

void ClientConnection::send_error(const std::string& message) {
	ensure_state(ClientState::AWAIT_RESPONSE);
	set_state(ClientState::WRITING_RESPONSE);
	begin_write(make_unique<NBMessageWriter<std::string>>(RESP_ERROR, message));
}

const BaseRequest& ClientConnection::get_request() const {
	ensure_state(ClientState::AWAIT_RESPONSE);
	return *request;
}

void ClientConnection::reset() {
	request.reset();
	set_state(ClientState::IDLE);
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

WorkerConnection::WorkerConnection(std::unique_ptr<BinaryFDStream> socket, const std::shared_ptr<Node> &node) :
	BaseConnection(WorkerState::IDLE, std::move(socket)), node(node) {
}

WorkerConnection::~WorkerConnection() {
}

void WorkerConnection::process_command(uint8_t cmd) {
	ensure_state(WorkerState::PROCESSING, WorkerState::WAITING_DELIVERY);

	switch (cmd) {
		case RESP_RESULT_READY: {
			// Read delivery id
			set_state(WorkerState::DONE);
			break;
		}
		case RESP_DELIVERY_READY: {
			set_state(WorkerState::READING_DELIVERY_ID);
			begin_read( make_unique<NBFixedSizeReader>( sizeof(uint64_t) ) );
			break;
		}
		case CMD_QUERY_CACHE: {
			set_state(WorkerState::READING_QUERY);
			begin_read( make_unique<NBBaseRequestReader>() );
			break;
		}
		case RESP_NEW_CACHE_ENTRY: {
			set_state(WorkerState::READING_ENTRY);
			begin_read( make_unique<NBNodeCacheRefReader>() );
			break;
		}
		case RESP_ERROR: {
			set_state(WorkerState::READING_ERROR);
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
	switch (get_state()) {
		case WorkerState::READING_DELIVERY_ID: {
			uint64_t delivery_id;
			reader.get_stream()->read(&delivery_id);
			result.reset(new DeliveryResponse(node->host, node->port, delivery_id));
			set_state(WorkerState::DELIVERY_READY);
			break;
		}
		case WorkerState::READING_QUERY:
			query.reset(new BaseRequest(*reader.get_stream()));
			set_state(WorkerState::QUERY_REQUESTED);
			break;
		case WorkerState::READING_ENTRY:
			new_entry.reset(new NodeCacheRef(*reader.get_stream()));
			set_state(WorkerState::NEW_ENTRY);
			break;
		case WorkerState::READING_ERROR:
			reader.get_stream()->read(&error_msg);
			set_state(WorkerState::ERROR);
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in WorkerConnection");
	}
}

void WorkerConnection::write_finished() {
	switch (get_state()) {
		case WorkerState::SENDING_REQUEST:
		case WorkerState::SENDING_QUERY_RESPONSE:
			set_state(WorkerState::PROCESSING);
			break;
		case WorkerState::SENDING_DELIVERY_QTY:
			set_state(WorkerState::WAITING_DELIVERY);
			break;
		default:
			throw IllegalStateException("Unexpected end of writing in WorkerConnection");
	}
}

// ACTIONS

void WorkerConnection::process_request(uint8_t command, const BaseRequest& request) {
	ensure_state(WorkerState::IDLE);
	set_state(WorkerState::SENDING_REQUEST);
	begin_write(
		make_unique<NBMessageWriter<BaseRequest>>(command, request, true));
}

void WorkerConnection::entry_cached() {
	ensure_state(WorkerState::NEW_ENTRY);
	// TODO: Do we need a confirmation of this
	set_state(WorkerState::PROCESSING);
}

void WorkerConnection::send_hit(const CacheRef& cr) {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	begin_write(
		make_unique<NBMessageWriter<CacheRef>>(RESP_QUERY_HIT, cr));
}

void WorkerConnection::send_partial_hit(const PuzzleRequest& pr) {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	begin_write(
		make_unique<NBMessageWriter<PuzzleRequest>>(RESP_QUERY_PARTIAL, pr));
}

void WorkerConnection::send_miss() {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	begin_write(make_unique<NBSimpleWriter<uint8_t>>(RESP_QUERY_MISS));
}

void WorkerConnection::send_delivery_qty(uint32_t qty) {
	ensure_state(WorkerState::DONE);
	set_state(WorkerState::SENDING_DELIVERY_QTY);
	begin_write(
		make_unique<NBMessageWriter<uint32_t>>(RESP_DELIVERY_QTY, qty));
}

void WorkerConnection::release() {
	ensure_state(WorkerState::DELIVERY_READY, WorkerState::ERROR);
	reset();
}

//
// GETTER
//

const NodeCacheRef& WorkerConnection::get_new_entry() const {
	ensure_state(WorkerState::NEW_ENTRY);
	return *new_entry;
}

const BaseRequest& WorkerConnection::get_query() const {
	ensure_state(WorkerState::QUERY_REQUESTED);
	return *query;
}

const DeliveryResponse& WorkerConnection::get_result() const {
	ensure_state(WorkerState::DELIVERY_READY);
	return *result;
}

const std::string& WorkerConnection::get_error_message() const {
	ensure_state(WorkerState::ERROR);
	return error_msg;
}

void WorkerConnection::reset() {
	error_msg = "";
	result.reset();
	new_entry.reset();
	query.reset();
	set_state(WorkerState::IDLE);
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

ControlConnection::ControlConnection(std::unique_ptr<BinaryFDStream> socket, const std::string &hostname) :
	BaseConnection(ControlState::READING_HANDSHAKE, std::move(socket)), hostname(hostname) {
	begin_read( make_unique<NBNodeHandshakeReader>() );
}

ControlConnection::~ControlConnection() {
}

void ControlConnection::process_command(uint8_t cmd) {
	ensure_state(ControlState::IDLE,ControlState::REORGANIZING,ControlState::STATS_REQUESTED);

	switch (cmd) {
		case RESP_REORG_ITEM_MOVED: {
			set_state(ControlState::READING_MOVE_RESULT);
			begin_read(make_unique<NBReorgMoveResultReader>());
			break;
		}
		case RESP_REORG_REMOVE_REQUEST: {
			set_state(ControlState::READING_REMOVE_REQUEST);
			begin_read(make_unique<NBTypedNodeCacheKeyReader>());
			break;
		}
		case RESP_REORG_DONE: {
			set_state(ControlState::REORG_FINISHED);
			break;
		}
		case RESP_STATS: {
			set_state(ControlState::READING_STATS);
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
	switch (get_state()) {
		case ControlState::READING_MOVE_RESULT:
			move_result.reset(new ReorgMoveResult(*reader.get_stream()));
			set_state(ControlState::MOVE_RESULT_READ);
			break;
		case ControlState::READING_REMOVE_REQUEST:
			remove_request.reset(new TypedNodeCacheKey(*reader.get_stream()));
			set_state(ControlState::REMOVE_REQUEST_READ);
			break;
		case ControlState::READING_STATS:
			stats.reset(new NodeStats(*reader.get_stream()));
			set_state(ControlState::STATS_RECEIVED);
			break;
		case ControlState::READING_HANDSHAKE:
			handshake.reset( new NodeHandshake(*reader.get_stream()));
			set_state(ControlState::HANDSHAKE_READ);
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ControlConnection");
	}
}

void ControlConnection::write_finished() {
	switch (get_state()) {
		case ControlState::SENDING_REORG:
			set_state(ControlState::REORGANIZING);
			break;
		case ControlState::SENDING_MOVE_CONFIRM:
		case ControlState::SENDING_REMOVE_CONFIRM:
			set_state(ControlState::REORGANIZING);
			break;
		case ControlState::SENDING_STATS_REQUEST:
			set_state(ControlState::STATS_REQUESTED);
			break;
		case ControlState::SENDING_HELLO:
			set_state(ControlState::IDLE);
			break;
		default:
			throw IllegalStateException("Unexpected end of reading in ControlConnection");
	}
}

// ACTIONS

void ControlConnection::confirm_handshake(std::shared_ptr<Node> node) {
	ensure_state(ControlState::HANDSHAKE_READ );
	this->node = node;
	set_state(ControlState::SENDING_HELLO);
	begin_write(
		make_unique<NBHelloWriter>( node->id, hostname )
	);
}

void ControlConnection::send_reorg(const ReorgDescription& desc) {
	ensure_state(ControlState::IDLE);
	set_state(ControlState::SENDING_REORG);
	begin_write(
		make_unique<NBMessageWriter<ReorgDescription>>(CMD_REORG, desc));
}

void ControlConnection::confirm_move() {
	ensure_state(ControlState::MOVE_RESULT_READ);
	set_state(ControlState::SENDING_MOVE_CONFIRM);
	begin_write(make_unique<NBSimpleWriter<uint8_t>>(CMD_MOVE_OK));
}

void ControlConnection::confirm_remove() {
	ensure_state(ControlState::REMOVE_REQUEST_READ);
	set_state(ControlState::SENDING_REMOVE_CONFIRM);
	begin_write(make_unique<NBSimpleWriter<uint8_t>>(CMD_REMOVE_OK));
}

void ControlConnection::send_get_stats() {
	ensure_state(ControlState::IDLE);
	set_state(ControlState::SENDING_STATS_REQUEST);
	begin_write(make_unique<NBSimpleWriter<uint8_t>>(CMD_GET_STATS));
}

void ControlConnection::release() {
	ensure_state(ControlState::REORG_FINISHED, ControlState::STATS_RECEIVED);
	reset();
}

//
// GETTER
//

const NodeHandshake& ControlConnection::get_handshake() const {
	ensure_state(ControlState::HANDSHAKE_READ );
	return *handshake;
}

const ReorgMoveResult& ControlConnection::get_move_result() const {
	ensure_state(ControlState::MOVE_RESULT_READ);
	return *move_result;
}

const TypedNodeCacheKey& ControlConnection::get_remove_request() const {
	ensure_state(ControlState::REMOVE_REQUEST_READ);
	return *remove_request;
}

const NodeStats& ControlConnection::get_stats() const {
	ensure_state(ControlState::STATS_RECEIVED);
	return *stats;
}

// Private stuff

void ControlConnection::reset() {
	move_result.reset();
	stats.reset();
	set_state(ControlState::IDLE);
}

const uint32_t ControlConnection::MAGIC_NUMBER;
const uint8_t ControlConnection::CMD_REORG;
const uint8_t ControlConnection::CMD_GET_STATS;
const uint8_t ControlConnection::CMD_MOVE_OK;
const uint8_t ControlConnection::CMD_REMOVE_OK;
const uint8_t ControlConnection::CMD_HELLO;
const uint8_t ControlConnection::RESP_REORG_ITEM_MOVED;
const uint8_t ControlConnection::RESP_REORG_REMOVE_REQUEST;
const uint8_t ControlConnection::RESP_REORG_DONE;
const uint8_t ControlConnection::RESP_STATS;

/////////////////////////////////////////////////
//
// DELIVERY-CONNECTION
//
/////////////////////////////////////////////////

DeliveryConnection::DeliveryConnection(std::unique_ptr<BinaryFDStream> socket) :
	BaseConnection(DeliveryState::IDLE, std::move(socket)), delivery_id(0), cache_key(CacheType::UNKNOWN,"", 0) {
}

DeliveryConnection::~DeliveryConnection() {
}

void DeliveryConnection::process_command(uint8_t cmd) {
	ensure_state( DeliveryState::IDLE, DeliveryState::AWAITING_MOVE_CONFIRM );

	switch (cmd) {
		case CMD_GET: {
			set_state(DeliveryState::READING_DELIVERY_REQUEST);
			begin_read(make_unique<NBFixedSizeReader>(sizeof(delivery_id)));
			break;
		}
		case CMD_GET_CACHED_ITEM: {
			set_state(DeliveryState::READING_CACHE_REQUEST);
			begin_read(make_unique<NBTypedNodeCacheKeyReader>());
			break;
		}
		case CMD_MOVE_ITEM: {
			set_state(DeliveryState::READING_MOVE_REQUEST);
			begin_read(make_unique<NBTypedNodeCacheKeyReader>());
			break;
		}
		case CMD_MOVE_DONE: {
			set_state(DeliveryState::MOVE_DONE);
			break;
		}
		default:
			// Unknown command
			throw NetworkException(concat("Unknown command on delivery connection: ", cmd));
	}
}

void DeliveryConnection::read_finished(NBReader& reader) {
	switch (get_state()) {
		case DeliveryState::READING_DELIVERY_REQUEST: {
			reader.get_stream()->read(&delivery_id);
			set_state(DeliveryState::DELIVERY_REQUEST_READ);
			break;
		}
		case DeliveryState::READING_CACHE_REQUEST: {
			cache_key = TypedNodeCacheKey(*reader.get_stream());
			set_state(DeliveryState::CACHE_REQUEST_READ);
			break;
		}
		case DeliveryState::READING_MOVE_REQUEST: {
			cache_key = TypedNodeCacheKey(*reader.get_stream());
			set_state(DeliveryState::MOVE_REQUEST_READ);
			break;
		}
		default:
			throw IllegalStateException("Unexpected end of reading in DeliveryConnection");
	}
}

void DeliveryConnection::write_finished() {
	switch (get_state()) {
		case DeliveryState::SENDING:
		case DeliveryState::SENDING_CACHE_ENTRY: {
			set_state(DeliveryState::IDLE);
			break;
		}
		case DeliveryState::SENDING_MOVE: {
			set_state(DeliveryState::AWAITING_MOVE_CONFIRM);
			break;
		}
		case DeliveryState::SENDING_ERROR: {
			set_state(DeliveryState::IDLE);
			break;
		}
		default:
			throw IllegalStateException("Unexpected end of writing in DeliveryConnection");
	}
}

const TypedNodeCacheKey& DeliveryConnection::get_key() const {
	ensure_state(DeliveryState::CACHE_REQUEST_READ,
				 DeliveryState::MOVE_REQUEST_READ,
				 DeliveryState::AWAITING_MOVE_CONFIRM,
				 DeliveryState::MOVE_DONE);
	return cache_key;
}

uint64_t DeliveryConnection::get_delivery_id() const {
	ensure_state(DeliveryState::DELIVERY_REQUEST_READ);
	return delivery_id;
}

template<typename T>
void DeliveryConnection::send(std::shared_ptr<const T> item) {
	ensure_state(DeliveryState::CACHE_REQUEST_READ, DeliveryState::DELIVERY_REQUEST_READ);
	set_state(DeliveryState::SENDING);
	begin_write (
		make_unique<NBMultiWriter>(
				make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
				get_data_writer(item)
		)
	);
}

template<typename T>
void DeliveryConnection::send_cache_entry(const MoveInfo& info,
		std::shared_ptr<const T> item) {
	ensure_state(DeliveryState::CACHE_REQUEST_READ);
	set_state(DeliveryState::SENDING_CACHE_ENTRY);
	begin_write(
		make_unique<NBMultiWriter>(
			make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
			make_unique<NBSimpleWriter<MoveInfo>>(info),
			get_data_writer(item)
		)
	);
}

template<typename T>
void DeliveryConnection::send_move(const CacheEntry& info,
		std::shared_ptr<const T> item) {

	ensure_state(DeliveryState::MOVE_REQUEST_READ);
	set_state(DeliveryState::SENDING_MOVE);
	begin_write(
		make_unique<NBMultiWriter>(
			make_unique<NBSimpleWriter<uint8_t>>(RESP_OK),
			make_unique<NBSimpleWriter<CacheEntry>>(info),
			get_data_writer(item)
		)
	);
}


void DeliveryConnection::send_error(const std::string& msg) {
	ensure_state( DeliveryState::CACHE_REQUEST_READ,
				  DeliveryState::DELIVERY_REQUEST_READ,
				  DeliveryState::MOVE_REQUEST_READ,
				  DeliveryState::SENDING,
				  DeliveryState::SENDING_MOVE);

	set_state(DeliveryState::SENDING_ERROR);
	begin_write(make_unique<NBMessageWriter<std::string>>(RESP_ERROR, msg));
}

void DeliveryConnection::release() {
	ensure_state(DeliveryState::MOVE_DONE);
	set_state( DeliveryState::IDLE );
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


template class BaseConnection<ClientState>;
template class BaseConnection<WorkerState>;
template class BaseConnection<ControlState>;
template class BaseConnection<DeliveryState>;

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

