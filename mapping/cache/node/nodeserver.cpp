/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/node/node_manager.h"
#include "cache/node/nodeserver.h"
#include "cache/node/delivery.h"
#include "cache/index/indexserver.h"
#include "cache/priv/connection.h"
#include "cache/priv/transfer.h"
#include "cache/manager.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/log.h"
#include "util/nio.h"
#include <sstream>

#include <sys/select.h>
#include <sys/socket.h>

////////////////////////////////////////////////////////////
//
// NODE SERVER
//
////////////////////////////////////////////////////////////

NodeServer::NodeServer(std::unique_ptr<NodeCacheManager> manager, uint32_t my_port, std::string index_host, uint32_t index_port,
	int num_threads) :
	shutdown(false), workers_up(false), my_id(-1), my_port(my_port), index_host(index_host), index_port(
		index_port), num_treads(num_threads), delivery_manager(my_port,*manager), manager(std::move(manager) ) {
	this->manager->set_self_port(my_port);
}

void NodeServer::worker_loop() {
	while (workers_up && !shutdown) {
		try {
			// Setup index connection
			BinaryFDStream sock(index_host.c_str(), index_port,true);
			BinaryStream &stream = sock;
			buffered_write(sock,WorkerConnection::MAGIC_NUMBER,my_id);
			manager->set_index_connection(&sock);

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
					auto msg = concat("Unexpected error while processing request: ", e.what());
					buffered_write(stream,WorkerConnection::RESP_ERROR, msg);
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
	ExecTimer t("RequestProcessing");
	Log::debug("Processing command: %d", cmd);
	switch (cmd) {
		case WorkerConnection::CMD_CREATE: {
			BaseRequest cr(stream);
			Log::debug("Processing create-request: %s", cr.to_string().c_str());
			process_create_request(stream,cr);
			break;
		}
		case WorkerConnection::CMD_PUZZLE: {
			PuzzleRequest pr(stream);
			Log::debug("Processing puzzle-request: %s", pr.to_string().c_str());
			process_puzzle_request(stream,pr);
			break;
		}
		case WorkerConnection::CMD_DELIVER: {
			DeliveryRequest dr(stream);
			Log::debug("Processing delivery-request: %s", dr.to_string().c_str());
			process_delivery_request(stream,dr);
			break;
		}
		default: {
			Log::error("Unknown command from index-server: %d. Dropping connection.", cmd);
			throw NetworkException(concat("Unknown command from index-server", cmd));
		}
	}
	Log::debug("Finished processing command: %d", cmd);
}

void NodeServer::process_create_request(BinaryStream& index_stream,
		const BaseRequest& request) {
	ExecTimer t("RequestProcessing.create");
	auto op = GenericOperator::fromJSON(request.semantic_id);

	QueryProfiler profiler;
	switch ( request.type ) {
		case CacheType::RASTER: {
			auto res = op->getCachedRaster( request.query, profiler );
			finish_request( index_stream, std::shared_ptr<const GenericRaster>(res.release()) );
			break;
		}
		case CacheType::POINT: {
			auto res = op->getCachedPointCollection( request.query, profiler );
			finish_request( index_stream, std::shared_ptr<const PointCollection>(res.release()) );
			break;
		}
		case CacheType::LINE: {
			auto res = op->getCachedLineCollection(request.query, profiler );
			finish_request( index_stream, std::shared_ptr<const LineCollection>(res.release()) );
			break;
		}
		case CacheType::POLYGON: {
			auto res = op->getCachedPolygonCollection(request.query, profiler );
			finish_request( index_stream, std::shared_ptr<const PolygonCollection>(res.release()) );
			break;
		}
		case CacheType::PLOT: {
			auto res = op->getCachedPlot(request.query, profiler );
			finish_request( index_stream, std::shared_ptr<const GenericPlot>(res.release()) );
			break;
		}
		default:
			throw ArgumentException(concat("Type ", (int) request.type, " not supported yet"));
	}
}

void NodeServer::process_puzzle_request(BinaryStream& index_stream,
		const PuzzleRequest& request) {
	ExecTimer t("RequestProcessing.puzzle");
	QueryProfiler profiler;

	switch ( request.type ) {
		case CacheType::RASTER: {
			auto res = manager->get_raster_cache().process_puzzle(request,profiler);
			finish_request( index_stream, std::shared_ptr<const GenericRaster>(res.release()) );
			break;
		}
		case CacheType::POINT: {
			auto res = manager->get_point_cache().process_puzzle(request,profiler);
			finish_request( index_stream, std::shared_ptr<const PointCollection>(res.release()) );
			break;
		}
		case CacheType::LINE: {
			auto res = manager->get_line_cache().process_puzzle(request,profiler);
			finish_request( index_stream, std::shared_ptr<const LineCollection>(res.release()) );
			break;
		}
		case CacheType::POLYGON: {
			auto res = manager->get_polygon_cache().process_puzzle(request,profiler);
			finish_request( index_stream, std::shared_ptr<const PolygonCollection>(res.release()) );
			break;
		}
		case CacheType::PLOT: {
			auto res = manager->get_plot_cache().process_puzzle(request,profiler);
			finish_request( index_stream, std::shared_ptr<const GenericPlot>(res.release()) );
			break;
		}
		default:
			throw ArgumentException(concat("Type ", (int) request.type, " not supported yet"));
	}
}

void NodeServer::process_delivery_request(BinaryStream& index_stream,
		const DeliveryRequest& request) {
	ExecTimer t("RequestProcessing.delivery");
	NodeCacheKey key(request.semantic_id,request.entry_id);

	switch ( request.type ) {
		case CacheType::RASTER:
			finish_request( index_stream, manager->get_raster_cache().get_ref(key) );
			break;
		case CacheType::POINT:
			finish_request( index_stream, manager->get_point_cache().get_ref(key) );
			break;
		case CacheType::LINE:
			finish_request( index_stream, manager->get_line_cache().get_ref(key) );
			break;
		case CacheType::POLYGON:
			finish_request( index_stream, manager->get_polygon_cache().get_ref(key) );
			break;
		case CacheType::PLOT:
			finish_request( index_stream, manager->get_plot_cache().get_ref(key) );
			break;
		default:
			throw ArgumentException(concat("Type ", (int) request.type, " not supported yet"));
	}
}


template<typename T>
void NodeServer::finish_request(BinaryStream& stream,
		const std::shared_ptr<const T>& item) {
	ExecTimer t("RequestProcessing.finish");

	Log::debug("Processing request finished. Asking for delivery-qty");
	stream.write(WorkerConnection::RESP_RESULT_READY);
	uint8_t cmd_qty;
	uint32_t qty;

	stream.read(&cmd_qty);
	if (cmd_qty != WorkerConnection::RESP_DELIVERY_QTY)
		throw ArgumentException(
			concat("Expected command ", WorkerConnection::RESP_DELIVERY_QTY, " but received ", cmd_qty));
	stream.read(&qty);
	uint64_t delivery_id = delivery_manager.add_delivery(item, qty);

	Log::debug("Sending delivery_id.");
	buffered_write(stream,WorkerConnection::RESP_DELIVERY_READY,delivery_id);
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

//
// Constrol connection
//

void NodeServer::process_control_command(uint8_t cmd, BinaryStream &stream) {
	switch (cmd) {
		case ControlConnection::CMD_REORG: {
			Log::debug("Received reorg command.");
			ReorgDescription d(stream);
			for (auto &rem_item : d.get_removals()) {
				handle_reorg_remove_item(rem_item, stream);
			}
			for (auto &move_item : d.get_moves()) {
				handle_reorg_move_item(move_item, stream);
			}
			stream.write(ControlConnection::RESP_REORG_DONE);
			break;
		}
		case ControlConnection::CMD_GET_STATS: {
			Log::debug("Received stats-request.");
			NodeStats stats = manager->get_stats();
			buffered_write(stream,ControlConnection::RESP_STATS,stats);
			break;
		}
		default: {
			Log::error("Unknown control-command from index-server: %d. Dropping control-connection.", cmd);
			throw NetworkException("Unknown control-command from index-server");
		}
	}
}

void NodeServer::handle_reorg_remove_item(const TypedNodeCacheKey &item, BinaryStream &index_stream) {
	Log::debug("Removing item from cache. Key: %s", item.to_string().c_str() );

	uint8_t iresp;
	buffered_write( index_stream, ControlConnection::RESP_REORG_REMOVE_REQUEST, item );

	index_stream.read(&iresp);

	if ( iresp == ControlConnection::CMD_REMOVE_OK ) {
		switch (item.type) {
			case CacheType::RASTER:
				manager->get_raster_cache().remove_local(item);
				break;
			case CacheType::POINT:
				manager->get_point_cache().remove_local(item);
				break;
			case CacheType::LINE:
				manager->get_line_cache().remove_local(item);
				break;
			case CacheType::POLYGON:
				manager->get_polygon_cache().remove_local(item);
				break;
			case CacheType::PLOT:
				manager->get_plot_cache().remove_local(item);
				break;
			default:
				throw ArgumentException(concat("Type ", (int) item.type, " not supported yet"));
		}
	}
	else {
		Log::error("Index did not confirm removal. Skipping. Response was: %d", iresp);
	}
}

void NodeServer::handle_reorg_move_item(const ReorgMoveItem& item, BinaryStream &index_stream) {
	uint64_t new_cache_id;

	Log::debug("Moving item from node %d to node %d. Key: %s:%d ", item.from_node_id, my_id,
		item.semantic_id.c_str(), item.entry_id);


	// Send move request
	try {
		uint8_t del_resp;

		BinaryFDStream us(item.from_host.c_str(), item.from_port, true);
		BinaryStream &del_stream = us;

		buffered_write(us,DeliveryConnection::MAGIC_NUMBER,DeliveryConnection::CMD_MOVE_ITEM,TypedNodeCacheKey(item));
		del_stream.read(&del_resp);
		switch (del_resp) {
			case DeliveryConnection::RESP_OK: {
				CacheEntry ce(del_stream);
				switch (item.type) {
					case CacheType::RASTER:
						new_cache_id = manager->get_raster_cache().put_local(
							item.semantic_id, GenericRaster::fromStream(del_stream), std::move(ce)).entry_id;
						break;
					case CacheType::POINT:
						new_cache_id = manager->get_point_cache().put_local(
							item.semantic_id, make_unique<PointCollection>(del_stream), std::move(ce)).entry_id;
					case CacheType::LINE:
						new_cache_id = manager->get_line_cache().put_local(
							item.semantic_id, make_unique<LineCollection>(del_stream), std::move(ce)).entry_id;
					case CacheType::POLYGON:
						new_cache_id = manager->get_polygon_cache().put_local(
							item.semantic_id, make_unique<PolygonCollection>(del_stream), std::move(ce)).entry_id;
					case CacheType::PLOT:
						new_cache_id = manager->get_plot_cache().put_local(
							item.semantic_id, GenericPlot::fromStream(del_stream), std::move(ce)).entry_id;
					default:
						throw ArgumentException(concat("Type ", (int) item.type, " not supported yet"));
				}
				break;
			}
			case DeliveryConnection::RESP_ERROR: {
				std::string msg;
				del_stream.read(&msg);
				throw NetworkException(
					concat("Could not move item", item.semantic_id, ":", item.entry_id, " from ",
						item.from_host, ":", item.from_port, ": ", msg));
			}
			default:
				throw NetworkException(concat("Received illegal response from delivery-node: ", del_resp));
		}
		confirm_move(item, new_cache_id, index_stream, del_stream);
	} catch (const NetworkException &ne) {
		Log::error("Could not process move: %s", ne.what());
		return;
	}
}

void NodeServer::confirm_move(const ReorgMoveItem& item, uint64_t new_id, BinaryStream &index_stream,
	BinaryStream &del_stream) {
	// Notify index
	uint8_t iresp;
	ReorgMoveResult rr(item.type, item.semantic_id, item.entry_id, item.from_node_id, my_id, new_id);
	buffered_write(index_stream,ControlConnection::RESP_REORG_ITEM_MOVED,rr);

	index_stream.read(&iresp);
	try {
		switch (iresp) {
			case ControlConnection::CMD_MOVE_OK: {
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
	this->control_connection.reset(new BinaryFDStream(index_host.c_str(), index_port,true));
	BinaryStream &stream = *this->control_connection;
	NodeHandshake hs = manager->create_handshake();

	Log::debug("Sending hello to index-server");
	// Say hello
	buffered_write(stream,ControlConnection::MAGIC_NUMBER,hs);

	Log::debug("Waiting for response from index-server");
	// Read node-id
	uint8_t resp;
	stream.read(&resp);
	if (resp == ControlConnection::CMD_HELLO) {
		std::string my_host;

		stream.read(&my_id);
		stream.read(&my_host);
		manager->set_self_host(my_host);
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
