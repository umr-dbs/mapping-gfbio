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



NewConnection::NewConnection( const std::string &hostname, int fd ) : hostname(hostname),
		stream( make_unique<BinaryFDStream>(fd,fd,true)), buffer( make_unique<BinaryReadBuffer>()  ) {
	stream->makeNonBlocking();
}

int NewConnection::read_fd() const {
	if ( stream )
		return stream->getReadFD();
	throw IllegalStateException("Stream released already");
}

bool NewConnection::read() {
	if ( stream ) {
		stream->readNB(*buffer);
		return buffer->isRead();
	}
	throw IllegalStateException("Stream released already");
}

BinaryStream& NewConnection::get_data() {
	if ( buffer->isRead() )
		return *buffer;
	else
		throw IllegalStateException("Buffer not fully read");
}

std::unique_ptr<BinaryFDStream> NewConnection::release_stream() {
	auto res = std::move(stream);
	stream.reset();
	return res;
}


template<typename StateType>
BaseConnection<StateType>::BaseConnection(StateType state, std::unique_ptr<BinaryFDStream> socket) :
	id(next_id++), state(state), faulty(false), socket(std::move(socket)), reader(new BinaryReadBuffer() ) {
}

template<typename StateType>
BaseConnection<StateType>::~BaseConnection() {
}

template<typename StateType>
bool BaseConnection<StateType>::input() {

	try {
		bool eof = socket->readNB(*reader, true );
		if ( eof ) {
			Log::debug("Connection closed %d", id);
			faulty = true;
		}
		else if ( reader->isRead() ) {
			process_command( reader->read<uint8_t>(), *reader );
			reader.reset( new BinaryReadBuffer() );
			return true;
		}
	} catch ( const NetworkException &ne ) {
		Log::warn("An error occured during read on connection %d: %s", id, ne.what());
		faulty = true;
		reader.reset(new BinaryReadBuffer());
	}
	return false;
}

template<typename StateType>
void BaseConnection<StateType>::output() {
	if ( writer ) {
		try {
			socket->writeNB(*writer);
			if ( writer->isFinished() ) {
				write_finished();
				writer.reset();
			}
		} catch ( const NetworkException &ne ) {
			Log::warn("An error occured during write on connection: %d", id);
			faulty = true;
			writer.reset();
		}
	}
	else
		throw IllegalStateException("Cannot trigger write while not in writing state.");
}

template<typename StateType>
void BaseConnection<StateType>::begin_write(std::unique_ptr<BinaryWriteBuffer> buffer) {
	if ( reader->isEmpty() && !writer ) {
		this->writer = std::move(buffer);
	}
	else
		throw IllegalStateException("Cannot start write. Another read or write action is in progress.");
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
bool BaseConnection<StateType>::is_writing() const {
	return writer != nullptr;
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

void ClientConnection::process_command(uint8_t cmd, BinaryStream& payload) {
	ensure_state(ClientState::IDLE);
	switch (cmd) {
		case CMD_GET:
			request.reset(new BaseRequest(payload));
			set_state(ClientState::AWAIT_RESPONSE);
			break;
		default:
			throw NetworkException(concat("Unknown command on client connection: ", cmd));
	}
}

void ClientConnection::write_finished() {
	switch (get_state()) {
		case ClientState::WRITING_RESPONSE:
			request.reset();
			set_state(ClientState::IDLE);
			break;
		default:
			throw IllegalStateException("Unexpected end of writing in ClientConnection");
	}
}

void ClientConnection::send_response(const DeliveryResponse& response) {
	ensure_state(ClientState::AWAIT_RESPONSE);
	set_state(ClientState::WRITING_RESPONSE);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_OK);
	buffer->write(response);
	begin_write(std::move(buffer));
}

void ClientConnection::send_error(const std::string& message) {
	ensure_state(ClientState::AWAIT_RESPONSE);
	set_state(ClientState::WRITING_RESPONSE);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_ERROR);
	buffer->write(message);
	begin_write(std::move(buffer));
}

const BaseRequest& ClientConnection::get_request() const {
	ensure_state(ClientState::AWAIT_RESPONSE);
	return *request;
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

void WorkerConnection::process_command(uint8_t cmd, BinaryStream &payload) {
	ensure_state(WorkerState::PROCESSING, WorkerState::WAITING_DELIVERY);

	switch (cmd) {
		case RESP_RESULT_READY: {
			// Send back qty!
			set_state(WorkerState::DONE);
			break;
		}
		case RESP_DELIVERY_READY: {
			uint64_t delivery_id = payload.read<uint64_t>();
			result.reset(new DeliveryResponse(node->host, node->port, delivery_id));
			set_state(WorkerState::DELIVERY_READY);
			break;
		}
		case CMD_QUERY_CACHE: {
			query.reset( new BaseRequest(payload) );
			set_state(WorkerState::QUERY_REQUESTED);
			break;
		}
		case RESP_NEW_CACHE_ENTRY: {
			new_entry.reset(new NodeCacheRef(payload));
			set_state(WorkerState::NEW_ENTRY);
			break;
		}
		case RESP_ERROR: {
			error_msg = payload.read<std::string>();
			set_state(WorkerState::ERROR);
			break;
		}
		default: {
			Log::error("Worker returned unknown code: %d. Terminating worker-connection.", cmd);
			throw NetworkException(concat("Unknown response from worker: ", cmd));
		}
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
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(command);
	buffer->write(request);
	begin_write(std::move(buffer));
}

void WorkerConnection::entry_cached() {
	ensure_state(WorkerState::NEW_ENTRY);
	// TODO: Do we need a confirmation of this
	set_state(WorkerState::PROCESSING);
}

void WorkerConnection::send_hit(const CacheRef& cr) {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_QUERY_HIT);
	buffer->write(cr);
	begin_write(std::move(buffer));
}

void WorkerConnection::send_partial_hit(const PuzzleRequest& pr) {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_QUERY_PARTIAL);
	buffer->write(pr);
	begin_write(std::move(buffer));
}

void WorkerConnection::send_miss() {
	ensure_state(WorkerState::QUERY_REQUESTED);
	set_state(WorkerState::SENDING_QUERY_RESPONSE);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_QUERY_MISS);
	begin_write(std::move(buffer));
}

void WorkerConnection::send_delivery_qty(uint32_t qty) {
	ensure_state(WorkerState::DONE);
	set_state(WorkerState::SENDING_DELIVERY_QTY);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_DELIVERY_QTY);
	buffer->write(qty);
	begin_write(std::move(buffer));
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

ControlConnection::ControlConnection(std::unique_ptr<BinaryFDStream> socket, std::shared_ptr<Node> node) :
	BaseConnection(ControlState::SENDING_HELLO, std::move(socket)), node(node) {
	this->node->control_connection = id;
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(CMD_HELLO);
	buffer->write(this->node->id);
	buffer->write(this->node->host);
	begin_write(std::move(buffer));
}

ControlConnection::~ControlConnection() {
}

void ControlConnection::process_command(uint8_t cmd, BinaryStream &payload) {
	switch (cmd) {
		case RESP_REORG_ITEM_MOVED: {
			ensure_state(ControlState::REORGANIZING);
			move_result.reset(new ReorgMoveResult(payload));
			set_state(ControlState::MOVE_RESULT_READ);
			break;
		}
		case RESP_REORG_REMOVE_REQUEST: {
			ensure_state(ControlState::REORGANIZING);
			remove_request.reset(new TypedNodeCacheKey(payload));
			set_state(ControlState::REMOVE_REQUEST_READ);
			break;
		}
		case RESP_REORG_DONE: {
			ensure_state(ControlState::REORGANIZING);
			set_state(ControlState::REORG_FINISHED);
			break;
		}
		case RESP_STATS: {
			ensure_state(ControlState::STATS_REQUESTED);
			stats.reset(new NodeStats(payload));
			set_state(ControlState::STATS_RECEIVED);
			break;
		}
		default: {
			throw NetworkException(
				concat("Received illegal command on control-connection for node: ", node->id));
		}
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

void ControlConnection::send_reorg(const ReorgDescription& desc) {
	ensure_state(ControlState::IDLE);
	set_state(ControlState::SENDING_REORG);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(CMD_REORG);
	buffer->write(desc);
	begin_write(std::move(buffer));
}

void ControlConnection::confirm_move() {
	ensure_state(ControlState::MOVE_RESULT_READ);
	set_state(ControlState::SENDING_MOVE_CONFIRM);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(CMD_MOVE_OK);
	begin_write(std::move(buffer));
}

void ControlConnection::confirm_remove() {
	ensure_state(ControlState::REMOVE_REQUEST_READ);
	set_state(ControlState::SENDING_REMOVE_CONFIRM);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(CMD_REMOVE_OK);
	begin_write(std::move(buffer));
}

void ControlConnection::send_get_stats() {
	ensure_state(ControlState::IDLE);
	set_state(ControlState::SENDING_STATS_REQUEST);
	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(CMD_GET_STATS);
	begin_write(std::move(buffer));
}

void ControlConnection::release() {
	ensure_state(ControlState::REORG_FINISHED, ControlState::STATS_RECEIVED);
	reset();
}

//
// GETTER
//

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
	remove_request.reset();
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

void DeliveryConnection::process_command(uint8_t cmd, BinaryStream &payload) {
	ensure_state( DeliveryState::IDLE, DeliveryState::AWAITING_MOVE_CONFIRM );

	switch (cmd) {
		case CMD_GET: {
			delivery_id = payload.read<uint64_t>();
			set_state(DeliveryState::DELIVERY_REQUEST_READ);
			break;
		}
		case CMD_GET_CACHED_ITEM: {
			cache_key = TypedNodeCacheKey(payload);
			set_state(DeliveryState::CACHE_REQUEST_READ);
			break;
		}
		case CMD_MOVE_ITEM: {
			cache_key = TypedNodeCacheKey(payload);
			set_state(DeliveryState::MOVE_REQUEST_READ);
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

	auto buffer = make_unique<BinaryWriteBufferWithSharedObject<const T>>(item);
	buffer->enableLinking();
	buffer->write(RESP_OK);
	write_data(*buffer,item);
	begin_write(std::move(buffer));
}

template<typename T>
void DeliveryConnection::send_cache_entry(const MoveInfo& info,
		std::shared_ptr<const T> item) {
	ensure_state(DeliveryState::CACHE_REQUEST_READ);
	set_state(DeliveryState::SENDING_CACHE_ENTRY);

	auto buffer = make_unique<BinaryWriteBufferWithSharedObject<const T>>(item);
	buffer->enableLinking();
	buffer->write(RESP_OK);
	buffer->write(info);
	write_data(*buffer,item);
	begin_write(std::move(buffer));
}

template<typename T>
void DeliveryConnection::send_move(const CacheEntry& info,
		std::shared_ptr<const T> item) {

	ensure_state(DeliveryState::MOVE_REQUEST_READ);
	set_state(DeliveryState::SENDING_MOVE);
	auto buffer = make_unique<BinaryWriteBufferWithSharedObject<const T>>(item);
	buffer->enableLinking();
	buffer->write(RESP_OK);
	buffer->write(info);
	write_data(*buffer,item);
	begin_write(std::move(buffer));
}


void DeliveryConnection::send_error(const std::string& msg) {
	ensure_state( DeliveryState::CACHE_REQUEST_READ,
				  DeliveryState::DELIVERY_REQUEST_READ,
				  DeliveryState::MOVE_REQUEST_READ,
				  DeliveryState::SENDING,
				  DeliveryState::SENDING_MOVE);

	set_state(DeliveryState::SENDING_ERROR);

	auto buffer = make_unique<BinaryWriteBuffer>();
	buffer->write(RESP_ERROR);
	buffer->write(msg);
	begin_write(std::move(buffer));
}

void DeliveryConnection::release() {
	ensure_state(DeliveryState::MOVE_DONE);
	set_state( DeliveryState::IDLE );
}

template<typename T>
void DeliveryConnection::write_data(BinaryStream& stream,
		std::shared_ptr<const T> &item) {
	stream.write( *item );
}

// Hack for GenericRaster since toStream is not const
template<>
void DeliveryConnection::write_data(BinaryStream& stream,
		std::shared_ptr<const GenericRaster> &item) {
	auto p = const_cast<GenericRaster*>(item.get());
	stream.write( *p );
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

template void DeliveryConnection::write_data( BinaryStream&, std::shared_ptr<const PointCollection>& );
template void DeliveryConnection::write_data( BinaryStream&, std::shared_ptr<const LineCollection>& );
template void DeliveryConnection::write_data( BinaryStream&, std::shared_ptr<const PolygonCollection>& );
template void DeliveryConnection::write_data( BinaryStream&, std::shared_ptr<const GenericPlot>& );

