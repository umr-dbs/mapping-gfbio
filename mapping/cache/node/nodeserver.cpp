/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/node/nodeserver.h"
#include "cache/node/delivery.h"
#include "cache/index/indexserver.h"
#include "cache/priv/connection.h"
#include "cache/priv/transfer.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/log.h"
#include <sstream>

#include <sys/select.h>
#include <sys/socket.h>

////////////////////////////////////////////////////////////
//
// NODE SERVER
//
////////////////////////////////////////////////////////////

NodeServer::NodeServer(std::string my_host, uint32_t my_port, std::string index_host, uint32_t index_port,
	int num_threads) :
	shutdown(false), workers_up(false), my_id(-1), my_host(my_host), my_port(my_port), index_host(index_host), index_port(
		index_port), num_treads(num_threads), delivery_manager(my_port) {
}

void NodeServer::worker_loop() {
	while (workers_up && !shutdown) {
		try {
			// Setup index connection
			UnixSocket sock(index_host.c_str(), index_port);
			BinaryStream &stream = sock;
			uint32_t magic = WorkerConnection::MAGIC_NUMBER;
			stream.write(magic);
			stream.write(my_id);
			CacheManager::remote_connection = &sock;

			Log::debug("Worker connected to index-server");

			while (workers_up && !shutdown) {
				try {
					uint8_t cmd;
					if (CacheCommon::read(&cmd, sock, 2, true))
						process_worker_command(cmd, stream);
					else {
						Log::info("Disconnect on worker.");
						break;
					}
				} catch (const TimeoutException &te) {
					//Log::trace("Read on worker-connection timed out. Trying again");
				} catch (const InterruptedException &ie) {
					Log::info("Read on worker-connection interrupted. Trying again.");
				} catch (const NetworkException &ne) {
					// Re-throw network-error to outer catch.
					throw;
				} catch (const std::exception &e) {
					Log::error("Unexpected error while processing request: %s", e.what());
					stream.write(WorkerConnection::RESP_ERROR);
					stream.write(concat("Unexpected error while processing request: ", e.what()));
				}
			}
		} catch (const NetworkException &ne) {
			Log::info("Worker lost connection to index... Reconnecting. Reason: %s", ne.what());
		}
		std::this_thread::sleep_for(std::chrono::seconds(2));
	}
	Log::info("Worker done.");
}

void NodeServer::process_worker_command(uint8_t cmd, BinaryStream& stream) {
	Log::debug("Processing command: %d", cmd);
	switch (cmd) {
		case WorkerConnection::CMD_CREATE_RASTER:
		case WorkerConnection::CMD_PUZZLE_RASTER:
		case WorkerConnection::CMD_DELIVER_RASTER:
			process_raster_request(cmd, stream);
			break;
		default: {
			Log::error("Unknown command from index-server: %d. Dropping connection.", cmd);
			throw NetworkException(concat("Unknown command from index-server", cmd));
		}
	}
	Log::debug("Finished processing command: %d", cmd);
}

void NodeServer::process_raster_request(uint8_t cmd, BinaryStream& stream) {
	std::shared_ptr<GenericRaster> result;
	switch (cmd) {
		case WorkerConnection::CMD_CREATE_RASTER: {
			BaseRequest rr(stream);
			QueryProfiler profiler;
			Log::debug("Processing request: %s", rr.to_string().c_str());
			auto tmp = GenericOperator::fromJSON(rr.semantic_id)->getCachedRaster(rr.query, profiler,
				GenericOperator::RasterQM::LOOSE);
			result = std::shared_ptr<GenericRaster>(tmp.release());
			break;
		}
		case WorkerConnection::CMD_PUZZLE_RASTER: {
			PuzzleRequest rr(stream);
			Log::debug("Processing request: %s", rr.to_string().c_str());
			auto tmp = CacheManager::process_raster_puzzle(rr, my_host, my_port);
			Log::debug("Adding puzzled raster to cache.");
			CacheManager::getInstance().put_raster(rr.semantic_id, tmp);
			result = std::shared_ptr<GenericRaster>(tmp.release());
			break;
		}

		case WorkerConnection::CMD_DELIVER_RASTER: {
			DeliveryRequest rr(stream);
			Log::debug("Processing request: %s", rr.to_string().c_str());
			NodeCacheKey key(rr.semantic_id, rr.entry_id);
			result = CacheManager::getInstance().get_raster_ref(key);
			break;
		}
	}
	Log::debug("Processing request finished. Asking for delivery-qty");
	stream.write(WorkerConnection::RESP_RESULT_READY);
	uint8_t cmd_qty;
	uint32_t qty;
	stream.read(&cmd_qty);
	stream.read(&qty);
	if (cmd_qty != WorkerConnection::RESP_DELIVERY_QTY)
		throw ArgumentException(
			concat("Expected command ", WorkerConnection::RESP_DELIVERY_QTY, " but received ", cmd_qty));

	uint64_t delivery_id = delivery_manager.add_raster_delivery(result, qty);
	Log::debug("Sending delivery_id.");
	stream.write(WorkerConnection::RESP_DELIVERY_READY);
	stream.write(delivery_id);
}

void NodeServer::run() {
	Log::info("Starting Node-Server");

	delivery_thread = delivery_manager.run_async();
	uint8_t cmd;

	while (!shutdown) {
		try {
			setup_control_connection();

			workers_up = true;
			for (int i = 0; i < num_treads; i++)
				workers.push_back(make_unique<std::thread>(&NodeServer::worker_loop, this));

			// Read on control
			while (!shutdown) {
				try {
					if (CacheCommon::read(&cmd, *control_connection, 2, true))
						process_control_command(cmd, *control_connection);
					else {
						Log::info("Disconnect on control-connection. Reconnecting.");
						break;
					}
				} catch (const TimeoutException &te) {
				} catch (const InterruptedException &ie) {
					Log::info("Interrupt on read from control-connection.");
				} catch (const NetworkException &ne) {
					Log::error("Error reading on control-connection. Reconnecting");
					break;
				}
			}
			workers_up = false;
			Log::debug("Waiting for worker-threads to terminate.");
			for (auto &w : workers) {
				w->join();
			}
			workers.clear();
		} catch (const NetworkException &ne_c) {
			Log::warn("Could not connect to index-server. Retrying in 5s. Reason: %s", ne_c.what());
			std::this_thread::sleep_for(std::chrono::seconds(5));
		}
	}
	delivery_manager.stop();
	delivery_thread->join();
	Log::info("Node-Server done.");
}

void NodeServer::process_control_command(uint8_t cmd, BinaryStream &stream) {
	switch (cmd) {
		case ControlConnection::CMD_REORG: {
			Log::debug("Received reorg command.");
			ReorgDescription d(stream);
			for (auto &rem_item : d.get_removals()) {
				handle_reorg_remove_item(rem_item);
			}
			for (auto &move_item : d.get_moves()) {
				handle_reorg_move_item(move_item, stream);
			}
			stream.write(ControlConnection::RESP_REORG_DONE);
			break;
		}
		case ControlConnection::CMD_GET_STATS: {
			Log::debug("Received stats-request.");
			NodeStats stats = CacheManager::getInstance().get_stats();
			stream.write(ControlConnection::RESP_STATS);
			stats.toStream(stream);
			break;
		}
		default: {
			Log::error("Unknown control-command from index-server: %d. Dropping control-connection.", cmd);
			throw NetworkException("Unknown control-command from index-server");
		}
	}
}

void NodeServer::handle_reorg_remove_item(const ReorgRemoveItem &item) {
//	Log::debug("Removing item from cache. Key: %s:%d",item.semantic_id.c_str(), item.entry_id);
	switch (item.type) {
		case ReorgMoveItem::Type::RASTER:
			CacheManager::getInstance().remove_raster_local(item);
			break;
		default:
			throw ArgumentException(concat("Type ", (int) item.type, " not supported yet"));
	}
}

void NodeServer::handle_reorg_move_item(const ReorgMoveItem& item, BinaryStream &index_stream) {
	uint32_t new_cache_id;

	Log::debug("Moving item from node %d to node %d. Key: %s:%d ", item.from_node_id, my_id,
		item.semantic_id.c_str(), item.entry_id);

	std::unique_ptr<BinaryStream> del_stream;

	// Send move request
	try {
		uint8_t del_resp;
		del_stream = initiate_move(item);
		del_stream->read(&del_resp);

		switch (del_resp) {
			case DeliveryConnection::RESP_OK:
				switch (item.type) {
					case ReorgMoveItem::Type::RASTER:
						new_cache_id = CacheManager::getInstance().put_raster_local(item.semantic_id,
							GenericRaster::fromStream(*del_stream)).entry_id;
						break;
					default:
						throw ArgumentException(concat("Type ", (int) item.type, " not supported yet"));
				}
				break;
			case DeliveryConnection::RESP_ERROR: {
				std::string msg;
				del_stream->read(&msg);
				throw NetworkException(
					concat("Could not move item", item.semantic_id, ":", item.entry_id, " from ",
						item.from_host, ":", item.from_port, ": ", msg));
			}
			default:
				throw NetworkException(concat("Received illegal response from delivery-node: ", del_resp));
		}
	} catch (const NetworkException &ne) {
		Log::error("Could not initiate move: %s", ne.what());
		return;
	}
	confirm_move(item, new_cache_id, index_stream, *del_stream);
}

std::unique_ptr<BinaryStream> NodeServer::initiate_move(const ReorgMoveItem &item) {
	std::unique_ptr<BinaryStream> result = make_unique<UnixSocket>(item.from_host.c_str(), item.from_port);
	result->write(DeliveryConnection::MAGIC_NUMBER);

	uint8_t cmd;
	switch (item.type) {
		case ReorgMoveItem::Type::RASTER:
			cmd = DeliveryConnection::CMD_MOVE_RASTER;
			break;
		default:
			throw ArgumentException(concat("Type ", (int) item.type, " not supported yet"));
	}

	result->write(cmd);
	item.NodeCacheKey::toStream(*result);
	return result;
}

void NodeServer::confirm_move(const ReorgMoveItem& item, uint64_t new_id, BinaryStream &index_stream,
	BinaryStream &del_stream) {
	// Notify index
	uint8_t iresp;
	ReorgMoveResult rr(item.type, item.semantic_id, item.entry_id, item.from_node_id, my_id, new_id);
	index_stream.write(ControlConnection::RESP_REORG_ITEM_MOVED);
	rr.toStream(index_stream);
	index_stream.read(&iresp);

	try {
		switch (iresp) {
			case ControlConnection::CMD_REORG_ITEM_OK: {
				Log::debug("Reorg of item finished. Notifying delivery instance.");
				del_stream.write(DeliveryConnection::CMD_MOVE_DONE);
				break;
			}
			default: {
				Log::warn("Index could not handle reorg of: %s:%d", item.semantic_id.c_str(), item.entry_id);
				break;
			}
		}
	} catch (const NetworkException &ne) {
		Log::error("Could not confirm reorg of raster-item to delivery instance.");
	}
}

void NodeServer::setup_control_connection() {
	Log::info("Connecting to index-server: %s:%d", index_host.c_str(), index_port);

	// Establish connection
	this->control_connection.reset(new UnixSocket(index_host.c_str(), index_port));
	BinaryStream &stream = *this->control_connection;
	NodeHandshake hs = CacheManager::getInstance().get_handshake(my_host, my_port);

	Log::debug("Sending hello to index-server");
	// Say hello
	uint32_t magic = ControlConnection::MAGIC_NUMBER;
	stream.write(magic);
	hs.toStream(stream);

	Log::debug("Waiting for response from index-server");
	// Read node-id
	uint8_t resp;
	stream.read(&resp);
	if (resp == ControlConnection::CMD_HELLO) {
		stream.read(&my_id);
		Log::info("Successfuly connected to index-server. My Id is: %d", my_id);
	}
	else {
		std::ostringstream msg;
		msg << "Index returned unknown response-code to: " << resp << ".";
		throw NetworkException(msg.str());
	}
}

std::unique_ptr<std::thread> NodeServer::run_async() {
	return make_unique<std::thread>(&NodeServer::run, this);
}

void NodeServer::stop() {
	Log::info("Node-server shutting down.");
	shutdown = true;
}

NodeServer::~NodeServer() {
	stop();
}
