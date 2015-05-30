/*
 * common.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "util/log.h"
#include "raster/exceptions.h"

#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <errno.h>

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

int Common::getListeningSocket(int port, bool nonblock, int backlog) {
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
		throw NetworkException("getaddrinfo() failed");
	}

	// loop through all the results and bind to the first we can
	for (p = servinfo; p != nullptr; p = p->ai_next) {
		int type = nonblock ? p->ai_socktype | nonblock : p->ai_socktype;

		if ((sock = socket(p->ai_family, type,
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

//
// Connection class
//
SocketConnection::SocketConnection(int fd) : fd(fd) {
	stream.reset(new UnixSocket(fd, fd));
};

SocketConnection::SocketConnection(const char* host, int port) : fd(-1) {
	UnixSocket *sck = new UnixSocket(host,port);
	fd = sck->getReadFD();
	stream.reset( sck );
}

SocketConnection::~SocketConnection() {
}

void Common::writeRasterRequest(SocketConnection& con,
		const std::string& graph_json, const QueryRectangle& query,
		GenericOperator::RasterQM query_mode) {
	uint8_t qm = (query_mode == GenericOperator::RasterQM::EXACT) ? 1 : 0;
	con.stream->write(graph_json);
	query.toStream(*con.stream);
	con.stream->write(qm);
}
