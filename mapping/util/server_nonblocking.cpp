
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
NonblockingServer::Connection::Connection(NonblockingServer &server, int fd, int id)
	: server(server), fd(fd), id(id), state(State::INITIALIZING) {
	stream.reset( new BinaryFDStream(fd,fd) );
	stream->makeNonBlocking();
	// the client is supposed to send the first data, so we'll start reading.
	waitForData();
}

NonblockingServer::Connection::~Connection() {

}

void NonblockingServer::Connection::startProcessing() {
	if (state != State::READING_DATA)
		throw MustNotHappenException("Connection::processData() can only be called while in state READING_DATA");
	state = State::PROCESSING_DATA;
	processData(std::move(readbuffer));
}

void NonblockingServer::Connection::waitForData() {
	if (state != State::WRITING_DATA && state != State::INITIALIZING)
		throw MustNotHappenException("Connection::waitForData() can only be called while in state WRITING_DATA");
	writebuffer.reset(nullptr);
	readbuffer.reset( new BinaryReadBuffer() );
	state = State::READING_DATA;
}

void NonblockingServer::Connection::markAsClosed() {
	readbuffer.reset(nullptr);
	writebuffer.reset(nullptr);
	state = State::CLOSED;
}

void NonblockingServer::Connection::startWritingData(std::unique_ptr<BinaryWriteBuffer> new_writebuffer) {
	if (state != State::PROCESSING_DATA && state != State::PROCESSING_DATA_ASYNC && state != State::IDLE)
		throw MustNotHappenException("Connection::startWritingData() cannot be called in current state");
	readbuffer.reset(nullptr);
	writebuffer = std::move(new_writebuffer);
	state = State::WRITING_DATA;
}

void NonblockingServer::Connection::enqueueForAsyncProcessing() {
	throw MustNotHappenException("enqueueForAsyncProcessing() not yet implemented!");
}

void NonblockingServer::Connection::goIdle() {
	if (state != State::PROCESSING_DATA && state != State::PROCESSING_DATA_ASYNC)
		throw MustNotHappenException("Connection::goIdle() cannot be called in current state");
	readbuffer.reset(nullptr);
	state = State::IDLE;
}


void NonblockingServer::Connection::processDataAsync() {
	throw MustNotHappenException("processDataAsync not implemented on this server!");
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
	: next_id(1), listensocket(-1), connections(), stop_pipe(BinaryFDStream::PIPE) {

}

NonblockingServer::~NonblockingServer() {
	if (listensocket) {
		close(listensocket);
		listensocket = -1;
	}
}

void NonblockingServer::readNB(Connection &c) {
	try {
		auto is_eof = c.stream->readNB(*(c.readbuffer), true);
		if (is_eof) {
			c.markAsClosed();
			return;
		}
	}
	catch (const std::exception &e) {
		Log::error("%d: Exception during readNB: %s", c.id, e.what());
		c.markAsClosed();
		return;
	}

	if (c.readbuffer->isRead()) {
		try {
			c.startProcessing();
			/*
			// no response? We're done, close the connection.
			if (response == nullptr) {
				Log::trace("%d: no response, disconnecting client", c.id);
				return true;
			}
			c.writebuffer = std::move(response);
			*/
		}
		catch (const std::exception &e) {
			Log::error("%d: Exception when processing command: %s", c.id, e.what());
			// TODO: maybe the state changed to PROCESSING_ASYNC, can we really close it here?
			c.markAsClosed();
			return;
		}
	}
}

void NonblockingServer::writeNB(Connection &c) {
	try {
		c.stream->writeNB(*(c.writebuffer));
		if (c.writebuffer->isFinished()) {
			Log::debug("%d: response sent", c.id);
			c.waitForData();
		}
	}
	catch (const std::exception &e) {
		Log::error("%d: Exception during writeNB: %s", c.id, e.what());
		c.markAsClosed();
		return;
	}
}


void NonblockingServer::listen(int portnr) {
	listensocket = getListeningSocket(portnr);
}

void NonblockingServer::start() {
	if (!listensocket)
		throw ArgumentException("NonblockingServer: call listen() before start()");

	while (true) {
		struct timeval tv{60,0};
		fd_set readfds;
		fd_set writefds;
		int maxfd = 0;

		FD_ZERO(&readfds);
		FD_ZERO(&writefds);
		auto it = connections.begin();
		while (it != connections.end()) {
			auto &c = *it;
			int fd = c->fd;
			Connection::State state = c->state;
			if (state == Connection::State::WRITING_DATA)
				FD_SET(fd, &writefds);
			else if (state == Connection::State::READING_DATA)
				FD_SET(fd, &readfds);
			else if (state == Connection::State::CLOSED) {
				auto id = c->id;
				it = connections.erase(it);
				Log::info("%d: closing, %lu clients remain", id, connections.size());
				continue; // avoid the ++it
			}
			else
				continue;
			maxfd = std::max(fd, maxfd);
			++it;
		}
		maxfd = std::max(stop_pipe.getReadFD(), maxfd);
		FD_SET(stop_pipe.getReadFD(), &readfds);

		maxfd = std::max(listensocket, maxfd);
		FD_SET(listensocket, &readfds);

		auto res = select(maxfd+1, &readfds, &writefds, nullptr, &tv);
		if (res == 0) // timeout
			continue;
		if (res < 0) {
			if (errno == EINTR) // interrupted by signal
				continue;
			throw NetworkException(concat("select() call failed: ", strerror(errno)));
		}

		if (FD_ISSET(stop_pipe.getReadFD(), &readfds)) {
			Log::info("Stopping Server");
			return;
		}

		for (auto &c : connections) {
			bool needs_closing = false;
			Connection::State state = c->state;
			if (state == Connection::State::WRITING_DATA && FD_ISSET(c->fd, &writefds)) {
				writeNB(*c);
			}
			else if (state == Connection::State::READING_DATA && FD_ISSET(c->fd, &readfds)) {
				readNB(*c);
			}
		}

		if (FD_ISSET(listensocket, &readfds)) {
			struct sockaddr_storage remote_addr;
			socklen_t sin_size = sizeof(remote_addr);
			int new_fd = accept(listensocket, (struct sockaddr *) &remote_addr, &sin_size);
			if (new_fd == -1) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					continue;
				if (errno == ECONNABORTED)
					continue;

				throw NetworkException(concat("accept() call failed: ", strerror(errno)));
			}

			int one = 1;
			setsockopt(new_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
			connections.push_back( createConnection(new_fd, next_id++) );
		}
	}
}


void NonblockingServer::stop() {
	// if the loop is currently in a select() phase, this write will wake it up.
	Log::info("Sending signal to stop server");
	char c = 0;
	stop_pipe.write(c);
}
