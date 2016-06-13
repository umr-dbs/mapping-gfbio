/*
 * delivery.cpp
 *
 *  Created on: 03.07.2015
 *      Author: mika
 */

#include "cache/node/delivery.h"
#include "cache/common.h"
#include "cache/manager.h"
#include "util/make_unique.h"
#include "util/log.h"

#include <sys/select.h>
#include <sys/socket.h>

Delivery::Delivery(uint64_t id, uint16_t count, std::shared_ptr<const GenericRaster> raster, time_t expiration_time) :
	id(id), expiration_time(expiration_time), count(count), type(CacheType::RASTER), raster(raster) {
}

Delivery::Delivery(uint64_t id, uint16_t count, std::shared_ptr<const PointCollection> points, time_t expiration_time) :
	id(id), expiration_time(expiration_time), count(count), type(CacheType::POINT), points(points) {
}

Delivery::Delivery(uint64_t id, uint16_t count, std::shared_ptr<const LineCollection> lines, time_t expiration_time) :
	id(id), expiration_time(expiration_time), count(count), type(CacheType::LINE), lines(lines) {
}

Delivery::Delivery(uint64_t id, uint16_t count, std::shared_ptr<const PolygonCollection> polygons, time_t expiration_time):
	id(id), expiration_time(expiration_time), count(count), type(CacheType::POLYGON), polygons(polygons) {
}

Delivery::Delivery(uint64_t id, uint16_t count, std::shared_ptr<const GenericPlot> plot, time_t expiration_time):
	id(id), expiration_time(expiration_time), count(count), type(CacheType::PLOT), plot(plot) {
}

void Delivery::send(DeliveryConnection& connection) {
	if ( count == 0 )
		throw DeliveryException(concat("Cannot send deliver: ", id, ". Delivery count reached."));
	count--;
	switch ( type ) {
		case CacheType::RASTER: connection.send( raster ); break;
		case CacheType::POINT: connection.send( points ); break;
		case CacheType::LINE: connection.send( lines ); break;
		case CacheType::POLYGON: connection.send( polygons ); break;
		case CacheType::PLOT: connection.send( plot ); break;
		default:
			throw ArgumentException("Cannot send other types than raster.");
	}
}

////////////////////////////////////////////////////////////
//
// DELIVERY MANAGER
//
////////////////////////////////////////////////////////////

DeliveryManager::DeliveryManager(uint32_t listen_port, NodeCacheManager &manager) :
	shutdown(false), listen_port(listen_port), delivery_id(1), manager(manager), wakeup_pipe(BinaryStream::makePipe()) {
}

template <typename T>
uint64_t DeliveryManager::add_delivery(std::shared_ptr<const T> result, uint32_t count) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	uint64_t res = delivery_id++;
	deliveries.emplace( res, Delivery(res,count,result, CacheCommon::time_millis() + 30000) );
	Log::trace("Added delivery with id: %d", res);
	return res;
}

Delivery& DeliveryManager::get_delivery(uint64_t id) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	Log::trace("Getting delivery with id: %d", id);
	return deliveries.at(id);
}

void DeliveryManager::remove_expired_deliveries() {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	time_t now = CacheCommon::time_millis();
	auto iter = deliveries.begin();
	while ( iter != deliveries.end() ) {
		if ( iter->second.count == 0 || now >= iter->second.expiration_time )
			deliveries.erase(iter++);
		else
			iter++;
	}
}

std::unique_ptr<std::thread> DeliveryManager::run_async() {
	return make_unique<std::thread>(&DeliveryManager::run, this);
}

void DeliveryManager::stop() {
	Log::info("Delivery-manager shutting down.");
	shutdown = true;
	wakeup();
}

DeliveryManager::~DeliveryManager() {
	stop();
}

void DeliveryManager::wakeup() {
	BinaryWriteBuffer buffer;
	buffer.write('w');
	wakeup_pipe.write(buffer);
}


void DeliveryManager::run() {
	Log::info("Starting Delivery-Manager");
	int delivery_fd = CacheCommon::get_listening_socket(listen_port,true,SOMAXCONN);

	struct pollfd fds[0xffff];
	fds[0].fd = delivery_fd;
	fds[0].events = POLLIN;
	fds[1].fd = wakeup_pipe.getReadFD();
	fds[1].events = POLLIN;

	size_t num_fds;

	std::vector<std::unique_ptr<NewNBConnection>> new_cons;

	// Read on delivery-socket
	while (!shutdown) {
		// Prepare listen socket
		fds[0].revents = 0;
		// Prepare wakeup
		fds[1].revents = 0;

		num_fds = 2;

		// setup new connections
		auto nc_iter = new_cons.begin();
		while ( nc_iter != new_cons.end() ){
			if ( (*nc_iter)->is_faulty() )
				nc_iter = new_cons.erase(nc_iter);
			else {
				(*nc_iter)->prepare(&fds[num_fds++]);
				nc_iter++;
			}
		}

		// Setup active connections
		auto dciter = connections.begin();
			while (dciter != connections.end()) {
			DeliveryConnection &dc = **dciter;
			if ( dc.is_faulty() )
				dciter = connections.erase(dciter);
			else {
				dc.prepare(&fds[num_fds++]);
				dciter++;
			}
		}


		int poll_ret = poll(fds, num_fds, 1000 );
		if (poll_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
			exit(1);
		}
		else if (poll_ret > 0) {
			if ( fds[1].revents & POLLIN ) {
				// we have been woken, now we need to read any outstanding data or the pipe will remain readable
				char buf[1024];
				read(wakeup_pipe.getReadFD(), buf, 1024);
			}

			// Current connections
			// Action on delivery connections
			process_connections();

			// Handshake
			process_handshake(new_cons);

			// New delivery connection
			if ( fds[0].revents & POLLIN ) {
				struct sockaddr_storage remote_addr;
				socklen_t sin_size = sizeof(remote_addr);
				int new_fd = accept(delivery_fd, (struct sockaddr *) &remote_addr, &sin_size);
				if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK)
					Log::error("Accept failed: %d", strerror(errno));
				else if (new_fd > 0) {
					Log::debug("New connection established, fd: %d", new_fd);
					new_cons.push_back( make_unique<NewNBConnection>(&remote_addr,new_fd) );
				}
			}
			remove_expired_deliveries();
		}
	}
	close(delivery_fd);
	Log::info("Delivery-Manager done.");
}

void DeliveryManager::process_handshake(
		std::vector<std::unique_ptr<NewNBConnection> >& new_cons) {
	auto it = new_cons.begin();
	while (it != new_cons.end()) {
		auto &nc = **it;
		try {
			if ( nc.process() ) {
				auto &data = nc.get_data();
				uint32_t magic = data.read<uint32_t>();
				if (magic == DeliveryConnection::MAGIC_NUMBER) {
					std::unique_ptr<DeliveryConnection> dc = make_unique<DeliveryConnection>(nc.release_socket());
					Log::debug("New delivery-connection createdm, id: %d", dc->id);
					connections.push_back(std::move(dc));
				}
				else
					Log::warn("Received unknown magic-number: %d. Dropping connection.", magic);
				it = new_cons.erase(it);
			}
			else
				it++;
		} catch (const std::exception &e) {
			Log::error("Error on new connection: %s. Dropping.", e.what());
			it = new_cons.erase(it);
		}
	}

}

void DeliveryManager::process_connections() {
	for ( auto &dc : connections ) {
		if ( dc->process() ) {
			switch ( dc->get_state() ) {
				case DeliveryState::DELIVERY_REQUEST_READ: {
					uint64_t id = dc->get_delivery_id();
					try {
						auto &res = get_delivery(id);
						Log::debug("Sending delivery: %d", id);
						res.send(*dc);
					} catch ( const DeliveryException &dce ) {
						Log::info("Could send delivery: %s", dce.what());
						dc->send_error(dce.what());
					} catch (const std::out_of_range &oor) {
						Log::info("Received request for unknown delivery-id: %d", id);
						dc->send_error(concat("Invalid delivery id: ",id));
					}
					break;
				}
				case DeliveryState::CACHE_REQUEST_READ:
					handle_cache_request(*dc);
					break;
				case DeliveryState::MOVE_REQUEST_READ:
					handle_move_request(*dc);
					break;
				case DeliveryState::MOVE_DONE:
					handle_move_done(*dc);
					break;
				default:
					Log::trace("Nothing todo on delivery connection: %d", dc->id);
			}
		}
	}
}

void DeliveryManager::handle_cache_request(DeliveryConnection& dc) {
	auto &key = dc.get_key();
	try {
		Log::debug("Sending cache-entry: %s", key.to_string().c_str());
		switch ( key.type ) {
			case CacheType::RASTER: {
				auto e = manager.get_raster_cache().get(key);
				dc.send_cache_entry( *e, e->data );
				break;
			}
			case CacheType::POINT: {
				auto e = manager.get_point_cache().get(key);
				dc.send_cache_entry( *e, e->data );
				break;
			}
			case CacheType::LINE: {
				auto e = manager.get_line_cache().get(key);
				dc.send_cache_entry( *e, e->data );
				break;
			}
			case CacheType::POLYGON: {
				auto e = manager.get_polygon_cache().get(key);
				dc.send_cache_entry( *e, e->data );
				break;
			}
			case CacheType::PLOT: {
				auto e = manager.get_plot_cache().get(key);
				dc.send_cache_entry( *e, e->data );
				break;
			}
			default: throw ArgumentException(concat("Handling of type: ",(int)key.type," not supported"));
		}
	} catch (const NoSuchElementException &nse) {
		dc.send_error(concat("No cache-entry found for key: ", key.to_string()));
	}
}

void DeliveryManager::handle_move_request(DeliveryConnection& dc) {
	auto &key = dc.get_key();
	try {
		Log::debug("Moving cache-entry: %s", key.to_string().c_str());
		switch ( key.type ) {
			case CacheType::RASTER: {
				auto e = manager.get_raster_cache().get(key);
				dc.send_move( *e, e->data );
				break;
			}
			case CacheType::POINT: {
				auto e = manager.get_point_cache().get(key);
				dc.send_move( *e, e->data );
				break;
			}
			case CacheType::LINE: {
				auto e = manager.get_line_cache().get(key);
				dc.send_move( *e, e->data );
				break;
			}
			case CacheType::POLYGON: {
				auto e = manager.get_polygon_cache().get(key);
				dc.send_move( *e, e->data );
				break;
			}
			case CacheType::PLOT: {
				auto e = manager.get_plot_cache().get(key);
				dc.send_move( *e, e->data );
				break;
			}
			default:
				throw ArgumentException(concat("Handling of type: ",(int)key.type," not supported"));
		}
	} catch (const NoSuchElementException &nse) {
		dc.send_error(concat("No cache-entry found for key: ", key.to_string()));
	}
}

void DeliveryManager::handle_move_done(DeliveryConnection& dc) {
	auto &key = dc.get_key();
	Log::debug("Move of entry: %s confirmed. Dropping.", key.to_string().c_str());
	switch ( key.type ) {
		case CacheType::RASTER: manager.get_raster_cache().remove_local(key); break;
		case CacheType::POINT: manager.get_point_cache().remove_local(key); break;
		case CacheType::LINE: manager.get_line_cache().remove_local(key); break;
		case CacheType::POLYGON: manager.get_polygon_cache().remove_local(key); break;
		case CacheType::PLOT: manager.get_plot_cache().remove_local(key); break;
		default: throw ArgumentException(concat("Handling of type: ",(int)key.type," not supported"));
	}
	dc.finish_move();
}

template uint64_t DeliveryManager::add_delivery(std::shared_ptr<const GenericRaster>, uint32_t);
template uint64_t DeliveryManager::add_delivery(std::shared_ptr<const PointCollection>, uint32_t);
template uint64_t DeliveryManager::add_delivery(std::shared_ptr<const LineCollection>, uint32_t);
template uint64_t DeliveryManager::add_delivery(std::shared_ptr<const PolygonCollection>, uint32_t);
template uint64_t DeliveryManager::add_delivery(std::shared_ptr<const GenericPlot>, uint32_t);
