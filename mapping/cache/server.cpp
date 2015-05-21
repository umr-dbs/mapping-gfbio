/*
 * cacheserver.cpp
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

// project-stuff
#include "cache/server.h"

#include <stdlib.h>
#include <stdio.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>



void Connection::process() {
	CacheRequest::fromStream(*stream)->execute(*stream);
}

CacheServer::~CacheServer() {
}

void CacheServer::thread_loop() {
	while (!shutdown) {
		try {
			auto connection = queue.pop();
			Log::log(DEBUG,"Received task. Processing");
			connection->process();
			Log::log(DEBUG,"Processing finished.");
		} catch ( ShutdownException &se ) {
			Log::log(INFO,"Worker stopped.");
			break;
		}
	}
}

void CacheServer::main_loop() {
	int listensocket = getListeningSocket(listenport);
	Log::log(DEBUG, "cache-server: listening on port %d", listenport);

	while (!shutdown) {
		Log::log(DEBUG,"Waiting for incoming connection");
		struct timeval tv{2,0};
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(listensocket, &readfds);
		select(listensocket + 1, &readfds, nullptr, nullptr, &tv);
		if (FD_ISSET(listensocket, &readfds)) {
			struct sockaddr_storage remote_addr;
			socklen_t sin_size = sizeof(remote_addr);
			int new_fd = accept(listensocket, (struct sockaddr *) &remote_addr, &sin_size);
			if (new_fd == -1) {
				if (errno != EAGAIN)
					perror("accept");
				continue;
			}
			Log::log(DEBUG,"New connection established, adding to worker-queue.");
			std::unique_ptr<Connection> con( new Connection(new_fd) );
			queue.push( std::move(con) );
		}
	}
	Log::log(INFO, "cache-server done");
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
		if ((sock = socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK, p->ai_protocol)) == -1)
			continue;

		int yes = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
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
	Log::log(INFO,"Starting cache-server,");
	Log::log(INFO,"Firing up %d worker-threads", num_threads);
	for ( int i = 0; i < num_threads; i++ ) {
		workers.push_back( std::unique_ptr<std::thread>( new std::thread(&CacheServer::thread_loop, this) ) );
	}
	Log::log(INFO,"Starting main-loop", num_threads);
	main_loop();
}

std::unique_ptr<std::thread> CacheServer::runAsync() {
	Log::log(INFO,"Starting cache-server,");
	Log::log(INFO,"Firing up %d worker-threads", num_threads);
	for ( int i = 0; i < num_threads; i++ ) {
		workers.push_back( std::unique_ptr<std::thread>( new std::thread(&CacheServer::thread_loop, this) ) );
	}
	Log::log(INFO,"Starting main-loop");
	return std::unique_ptr<std::thread>( new std::thread(&CacheServer::main_loop,this) );
}

void CacheServer::stop() {
	Log::log(INFO,"Shutting down workers.");
	shutdown = true;
	queue.shutdown();
	for ( auto& t : workers ) {
		t->join();
	}
	workers.clear();
	Log::log(INFO,"Waiting for main_loop.");
}
