
#include "util/exceptions.h"
#include "util/server_nonblocking.h"
#include "util/log.h"

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <string.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <errno.h>


/*
 * Connection
 */
NonblockingServer::Connection::Connection(int fd, int id)
	: fd(fd), id(id) {
	stream.reset( new BinaryFDStream(fd,fd) );
	stream->makeNonBlocking();
	// the client is supposed to send the first data, so we'll start reading.
	readbuffer.reset( new BinaryReadBuffer() );
}

NonblockingServer::Connection::~Connection() {

}


/*
 * Helper Function
 */
static int getListeningSocket(int port, int backlog = 10) {
	int sock;
	struct addrinfo hints, *servinfo, *p;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	char portstr[16];
	snprintf(portstr, 16, "%d", port);
	int rv;
	if ((rv = getaddrinfo(nullptr, portstr, &hints, &servinfo)) != 0)
		throw NetworkException(concat("getaddrinfo() failed: ", gai_strerror(rv)));

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

/*
 * Nonblocking Server
 */
NonblockingServer::NonblockingServer()
	: next_id(1), listensocket(-1), connections() {

}

NonblockingServer::~NonblockingServer() {

}

bool NonblockingServer::readNB(Connection &c) {
	auto is_eof = c.stream->readNB(*(c.readbuffer), true);
	if (is_eof)
		return true;

	if (c.readbuffer->isRead()) {
		try {
			auto response = c.processRequest(*this, std::move(c.readbuffer));
			// no response? We're done, close the connection.
			if (response == nullptr)
				return true;
			c.writebuffer = std::move(response);
		}
		catch (const std::exception &e) {
			Log::error("%d: Exception when processing command: %s", c.id, e.what());
			return true;
		}
	}
	return false;
}

void NonblockingServer::writeNB(Connection &c) {
	c.stream->writeNB(*(c.writebuffer));
	if (c.writebuffer->isFinished()) {
		Log::debug("%d: response sent", c.id);
		c.writebuffer.reset(nullptr);
		c.readbuffer.reset( new BinaryReadBuffer() );
	}
}


void NonblockingServer::start(int portnr) {
	listensocket = getListeningSocket(portnr);

	std::string status, oldstatus;

    while (true) {
        struct timeval tv{60,0};
        fd_set readfds;
        fd_set writefds;
        int maxfd = 0;

        oldstatus = status;
        status = "";

        FD_ZERO(&readfds);
        for (auto &c : connections) {
        	int fd = c->fd;
    		maxfd = std::max(fd, maxfd);
        	if (c->writebuffer != nullptr) {
				FD_SET(fd, &writefds);
				status += "W";
        	}
        	else if (c->readbuffer != nullptr) {
				FD_SET(fd, &readfds);
				status += "R";
        	}
        	else
        		status += "?";
        }

        if (status != oldstatus)
        	Log::debug("Status: %s", status.c_str());

    	maxfd = std::max(listensocket, maxfd);
    	FD_SET(listensocket, &readfds);

        select(maxfd+1, &readfds, &writefds, nullptr, &tv);

        auto it = connections.begin();
        while (it != connections.end()) {
        	auto &c = *it;
    		bool needs_closing = false;
    		if (c->writebuffer != nullptr && FD_ISSET(c->fd, &writefds)) {
    			writeNB(*c);
    		}
    		else if (c->readbuffer != nullptr && FD_ISSET(c->fd, &readfds)) {
    			needs_closing = (readNB(*c) != 0);
        	}
    		if (needs_closing) {
    			auto id = c->id;
    			it = connections.erase(it);
    			Log::info("%d: closing, %lu clients remain", id, connections.size());
    		}
    		else
    			++it;
        }

        if (FD_ISSET(listensocket, &readfds)) {
			struct sockaddr_storage remote_addr;
			socklen_t sin_size = sizeof(remote_addr);
			int new_fd = accept(listensocket, (struct sockaddr *) &remote_addr, &sin_size);
			if (new_fd == -1) {
				if (errno != EAGAIN)
					perror("accept");
				continue;
			}

			int one = 1;
			setsockopt(new_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
			connections.push_back( createConnection(new_fd, next_id++) );
        }
    }
}
