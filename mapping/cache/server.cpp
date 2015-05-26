/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/server.h"
#include "operators/operator.h"

#include <stdlib.h>
#include <stdio.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

void Connection::process() {
	uint8_t cmd;
	if (!stream->read(&cmd, true)) {
		Log::debug("Disconnect on socket: %d", fd);
		throw DisconnectException("Connection closed");
	}

	Log::debug("Received command: %d", cmd);
	try {
		switch (cmd) {
		case CacheServer::COMMAND_GET_RASTER: {
			uint8_t qmval;
			std::string graphstr;

			QueryRectangle rect(*stream);
			stream->read(&graphstr);
			stream->read(&qmval);

			auto graph = GenericOperator::fromJSON(graphstr);
			GenericOperator::RasterQM querymode =
					(qmval == 0) ?
							GenericOperator::RasterQM::LOOSE :
							GenericOperator::RasterQM::EXACT;
			QueryProfiler profiler;
			auto result = graph->getCachedRaster(rect, profiler, querymode);
			uint8_t code = CacheServer::RESPONSE_OK;
			stream->write( code );
			result->toStream(*stream);
			break;
		}
		default:
			throw NetworkException("Unknown command.");
		}
	} catch ( OperatorException &oe ) {
		Log::warn("Operator caused exception: %s", oe.what());
		uint8_t code = CacheServer::RESPONSE_ERROR;
//		std::string err_msg = ;
		stream->write( code );
		stream->write( std::string(oe.what()) );
	}
}

CacheServer::~CacheServer() {
}

void CacheServer::thread_loop() {
	while (!shutdown) {
		try {
			auto connection = queue.pop();
			Log::debug("Received task. Processing");
			connection->process();
			Log::debug("Command processed. Releasing connection.");
			std::lock_guard<std::mutex> lg_cons(connection_mtx);
			connections.push_back( std::move(connection) );
		} catch (ShutdownException &se) {
			Log::info("Worker stopped.");
			break;
		} catch (DisconnectException &de) {
		} catch (std::exception &e ) {
			Log::warn("Error occured while processing request. Discarding connection. Reason: %s", e.what());
		}
	}
}

void CacheServer::main_loop() {
	int listensocket = getListeningSocket(listenport);
	Log::info("cache-server: listening on port %d", listenport);

	while (!shutdown) {
		Log::debug("Waiting for incoming connection");
		struct timeval tv { 2, 0 };
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(listensocket, &readfds);

		int maxfd = listensocket;

		// Lock while looping
		{
			std::lock_guard<std::mutex> lg_cons(connection_mtx);
			for (auto &e : connections) {
				FD_SET(e->fd, &readfds);
				maxfd = std::max(e->fd, maxfd);
			}
		}

		int sel_ret = select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
		if (sel_ret < 0 && errno != EINTR) {
			Log::error("Select returned error: %s", strerror(errno));
		}
		if (sel_ret < 0 && errno == EINTR) {
			Log::info("Exiting main_loop.");
			break;
		}
		else if (sel_ret > 0) {

			// Check reads
			{
				std::lock_guard<std::mutex> lg_cons(connection_mtx);
				auto it = connections.begin();
				while (it != connections.end()) {
					auto &c = *it;
					if (FD_ISSET(c->fd, &readfds)) {
						// move from idle to processing queue
						queue.push( c );
						it = connections.erase(it);
					}
					else
						++it;
				}
			}

			// New connection
			if (FD_ISSET(listensocket, &readfds)) {
				struct sockaddr_storage remote_addr;
				socklen_t sin_size = sizeof(remote_addr);
				int new_fd = accept(listensocket,
						(struct sockaddr *) &remote_addr, &sin_size);
				if (new_fd == -1) {
					if (errno != EAGAIN)
						perror("accept");
					continue;
				}
				Log::debug("New connection established on fd: %d", new_fd);
				std::lock_guard<std::mutex> lg_cons(connection_mtx);
				connections.push_back( std::unique_ptr<Connection>(new Connection(new_fd)) );
			}
		}
	}
	close(listensocket);
}

int CacheServer::getListeningSocket(int port, int backlog) {
	int sock;
	struct addrinfo hints, *servinfo, *p;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	char portstr[16];
	snprintf(portstr, 16, "%d", port);
	int rv;
	if ((rv = getaddrinfo(nullptr, portstr, &hints, &servinfo)) != 0) {
		//fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		throw NetworkException("getaddrinfo() failed");
	}

	// loop through all the results and bind to the first we can
	for (p = servinfo; p != nullptr; p = p->ai_next) {
		if ((sock = socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK,
				p->ai_protocol)) == -1)
			continue;

		int yes = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
				== -1) {
			freeaddrinfo(servinfo); // all done with this structure
			throw NetworkException("setsockopt() failed");
		}

		if (bind(sock, p->ai_addr, p->ai_addrlen) == -1) {
			close(sock);
			continue;
		}

		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (p == nullptr)
		throw NetworkException("failed to bind");

	if (listen(sock, backlog) == -1)
		throw NetworkException("listen() failed");

	return sock;
}

void CacheServer::run() {
	Log::info("Starting cache-server,");
	Log::info("Firing up %d worker-threads", num_threads);
	for (int i = 0; i < num_threads; i++) {
		workers.push_back(
				std::unique_ptr<std::thread>(
						new std::thread(&CacheServer::thread_loop, this)));
	}
	Log::info("Starting main-loop", num_threads);
	main_loop();
}

std::unique_ptr<std::thread> CacheServer::runAsync() {
	Log::info("Starting cache-server,");
	Log::info("Firing up %d worker-threads", num_threads);
	for (int i = 0; i < num_threads; i++) {
		workers.push_back(
				std::unique_ptr<std::thread>(
						new std::thread(&CacheServer::thread_loop, this)));
	}
	Log::info("Starting main-loop");
	return std::unique_ptr<std::thread>(
			new std::thread(&CacheServer::main_loop, this));
}

void CacheServer::stop() {
	Log::info("Shutting down workers.");
	shutdown = true;
	queue.shutdown();
	for (auto& t : workers) {
		t->join();
	}
	workers.clear();
}
