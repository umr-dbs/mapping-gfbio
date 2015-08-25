/*
 * delivery.cpp
 *
 *  Created on: 03.07.2015
 *      Author: mika
 */

#include "cache/node/delivery.h"
#include "cache/common.h"
#include "util/make_unique.h"
#include "util/log.h"

#include <sys/select.h>
#include <sys/socket.h>

Delivery::Delivery(uint64_t id, unsigned int count, std::shared_ptr<GenericRaster> raster) :
	id(id), creation_time(time(0)), count(count), type(Type::RASTER), raster(std::move(raster)) {
}

void Delivery::send(DeliveryConnection& connection) {
	count--;
	switch ( type ) {
		case Type::RASTER: {
			connection.send_raster( raster );
			break;
		}
		default: {
			Log::error("Currently only raster supported");
			throw ArgumentException("Cannot send other types than raster.");
		}
	}
}

Delivery::Delivery(Delivery&& d) :
	id(d.id), creation_time(d.creation_time),
	count(d.count), type(d.type), raster(std::move(d.raster)) {
}

////////////////////////////////////////////////////////////
//
// DELIVERY MANAGER
//
////////////////////////////////////////////////////////////

DeliveryManager::DeliveryManager(uint32_t listen_port) :
	shutdown(false), listen_port(listen_port), delivery_id(1) {
}

uint64_t DeliveryManager::add_raster_delivery(std::shared_ptr<GenericRaster> result, unsigned int count) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	uint64_t res = delivery_id++;
	deliveries.emplace(res, Delivery(res,count,result) );
	Log::trace("Added delivery with id: %d", res);
	return res;
}

Delivery& DeliveryManager::get_delivery(uint64_t id) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	Log::trace("Getting delivery with id: %d", id);
	return deliveries.at(id);
}

void DeliveryManager::remove_delivery(uint64_t id) {
	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	Log::trace("Removing delivery with id: %d", id);
	deliveries.erase(id);
}


void DeliveryManager::remove_expired_deliveries() {
	time_t now = time(nullptr);

	std::lock_guard<std::mutex> del_lock(delivery_mutex);
	auto iter = deliveries.begin();
	while ( iter != deliveries.end() ) {
		if ( difftime(now, iter->second.creation_time) >= 30 )
			deliveries.erase(iter++);
		else
			iter++;
	}
}

void DeliveryManager::run() {
	Log::info("Starting Delivery-Manager");
	int delivery_fd = CacheCommon::get_listening_socket(listen_port);

	std::vector<int> new_fds;

	// Read on delivery-socket
	while (!shutdown) {
		struct timeval tv { 2, 0 };
		fd_set readfds;
		fd_set writefds;
		FD_ZERO(&readfds);
		FD_ZERO(&writefds);
		FD_SET(delivery_fd, &readfds);

		int maxfd = delivery_fd;

		for (auto &fd : new_fds) {
			FD_SET(fd, &readfds);
			maxfd = std::max(maxfd, fd);
		}

		maxfd = std::max(maxfd, setup_fdset(&readfds,&writefds));

		int ret = select(maxfd + 1, &readfds, &writefds, nullptr, &tv);
		if (ret <= 0)
			continue;

		// Current connections
		// Action on delivery connections
		process_connections(&readfds,&writefds);

		// Handshake
		auto fd_it = new_fds.begin();
		while (fd_it != new_fds.end()) {
			if (FD_ISSET(*fd_it, &readfds)) {
				std::unique_ptr<UnixSocket> socket = make_unique<UnixSocket>(*fd_it, *fd_it);
				BinaryStream &stream = *socket;
				uint32_t magic;
				stream.read(&magic);
				if (magic == DeliveryConnection::MAGIC_NUMBER) {
					std::unique_ptr<DeliveryConnection> dc = make_unique<DeliveryConnection>(std::move(socket));
					Log::debug("New delivery-connection created on fd: %d", *fd_it);
					connections.push_back(std::move(dc));
				}
				else {
					Log::warn("Received unknown magic-number: %d. Dropping connection.", magic);
				}
				fd_it = new_fds.erase(fd_it);
			}
			else {
				++fd_it;
			}
		}

		// New delivery connection
		if (FD_ISSET(delivery_fd, &readfds)) {
			struct sockaddr_storage remote_addr;
			socklen_t sin_size = sizeof(remote_addr);
			int new_fd = accept4(delivery_fd, (struct sockaddr *) &remote_addr, &sin_size, SOCK_NONBLOCK);
			if (new_fd == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
				Log::error("Accept failed: %d", strerror(errno));
			}
			else if (new_fd > 0) {
				Log::debug("New delivery-connection accepted on fd: %d", new_fd);
				new_fds.push_back(new_fd);
			}
		}

		remove_expired_deliveries();
	}
	close(delivery_fd);
	Log::info("Delivery-Manager done.");
}

int DeliveryManager::setup_fdset(fd_set* readfds, fd_set* write_fds) {
	int maxfd = -1;
	auto dciter = connections.begin();
	while (dciter != connections.end()) {
		DeliveryConnection &dc = **dciter;
		if ( dc.is_faulty() )
			dciter = connections.erase(dciter);
		else if ( dc.is_writing() ) {
			FD_SET(dc.get_write_fd(), write_fds);
			maxfd = std::max(maxfd, dc.get_write_fd());
			dciter++;
		}
		else {
			FD_SET(dc.get_read_fd(), readfds);
			maxfd = std::max(maxfd, dc.get_read_fd());
			dciter++;
		}
	}
	return maxfd;
}

void DeliveryManager::process_connections(fd_set* readfds, fd_set* writefds) {
	for ( auto &dc : connections ) {
		if ( dc->is_writing() && FD_ISSET(dc->get_write_fd(), writefds) ) {
			dc->do_write();
		}
		else if ( !dc->is_writing() && FD_ISSET(dc->get_read_fd(), readfds)) {
			dc->input();
			// Skip faulty/reading connections
			if ( dc->is_faulty() || dc->is_reading() )
				continue;
			switch ( dc->get_state() ) {
				case DeliveryConnection::State::DELIVERY_REQUEST_READ: {
					uint64_t id = dc->get_delivery_id();
					try {
						auto &res = get_delivery(id);
						Log::debug("Sending delivery: %d", id);
						res.send(*dc);
						if ( res.count == 0 )
							remove_delivery(res.id);
					} catch (std::out_of_range &oor) {
						Log::info("Received request for unknown delivery-id: %d", id);
						dc->send_error(concat("Invalid delivery id: ",id));
					}
					break;
				}
				case DeliveryConnection::State::RASTER_CACHE_REQUEST_READ: {
					auto &key = dc->get_key();
					try {
						Log::debug("Sending cache-entry: %s:%d", key.semantic_id.c_str(), key.entry_id);
						dc->send_raster( CacheManager::getInstance().get_raster_ref(key) );
					} catch (NoSuchElementException &nse) {
						dc->send_error(concat("No cache-entry found for key: ", key.semantic_id, ":", key.entry_id));
					}
					break;
				}
				case DeliveryConnection::State::RASTER_MOVE_REQUEST_READ: {
					auto &key = dc->get_key();
					try {
						Log::debug("Moving cache-entry: %s:%d", key.semantic_id.c_str(), key.entry_id);
						dc->send_raster_move( CacheManager::getInstance().get_raster_ref(key) );
					} catch (NoSuchElementException &nse) {
						dc->send_error(concat("No cache-entry found for key: ", key.semantic_id, ":", key.entry_id));
					}
					break;
				}
				case DeliveryConnection::State::MOVE_DONE: {
					auto &key = dc->get_key();
					Log::debug("Move of entry: %s:%d confirmed. Dropping.", key.semantic_id.c_str(), key.entry_id);
					CacheManager::getInstance().remove_raster_local(key);
					dc->release();
					break;
				}
				default: {
					Log::trace("Nothing todo on delivery connection: %d", dc->id);
				}
			}
		}
	}
}

std::unique_ptr<std::thread> DeliveryManager::run_async() {
	return make_unique<std::thread>(&DeliveryManager::run, this);
}

void DeliveryManager::stop() {
	Log::info("Delivery-manager shutting down.");
	shutdown = true;
}

DeliveryManager::~DeliveryManager() {
	stop();
}

