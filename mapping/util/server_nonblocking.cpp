
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
	: fd(fd), state(State::INITIALIZING), is_closed(false), server(server), id(id) {
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
	// processData() can either send a reply (WRITING_DATA), process async or go idle
	if (state == Connection::State::PROCESSING_DATA)
		throw MustNotHappenException("processData() did not change the state, expected PROCESS_ASYNC, IDLE or a reply");
}

void NonblockingServer::Connection::waitForData() {
	if (state != State::WRITING_DATA && state != State::INITIALIZING)
		throw MustNotHappenException("Connection::waitForData() can only be called while in state WRITING_DATA");
	writebuffer.reset(nullptr);
	readbuffer.reset( new BinaryReadBuffer() );
	state = State::READING_DATA;
}

void NonblockingServer::Connection::close() {
	is_closed = true;
	if (state != State::PROCESSING_DATA_ASYNC) {
		// we must not remove them while another thread may be using the connection.
		readbuffer.reset(nullptr);
		writebuffer.reset(nullptr);
		stream->close();
	}
}

void NonblockingServer::Connection::startWritingData(std::unique_ptr<BinaryWriteBuffer> new_writebuffer) {
	State old_state = state;
	if (old_state != State::PROCESSING_DATA && old_state != State::PROCESSING_DATA_ASYNC && old_state != State::IDLE)
		throw MustNotHappenException("Connection::startWritingData() cannot be called in current state");
	readbuffer.reset(nullptr);
	writebuffer = std::move(new_writebuffer);
	state = State::WRITING_DATA;
	if (old_state != State::PROCESSING_DATA)
		server.wake();
}

void NonblockingServer::Connection::enqueueForAsyncProcessing() {
	if (state != State::PROCESSING_DATA)
		throw MustNotHappenException("Connection::enqueueForAsyncProcessing() can only be called while in state PROCESSING");
	if (!server.running || server.num_workers == 0)
		throw MustNotHappenException("Connection::enqueueForAsyncProcessing(): server does not have any worker threads");

	server.enqueueTask(this);
}

void NonblockingServer::Connection::goIdle() {
	if (state != State::PROCESSING_DATA && state != State::PROCESSING_DATA_ASYNC)
		throw MustNotHappenException("Connection::goIdle() cannot be called in current state");
	readbuffer.reset(nullptr);
	state = State::IDLE;
}


void NonblockingServer::Connection::processDataAsync() {
	throw MustNotHappenException("processDataAsync not implemented on this connection!");
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
	: num_workers(0), next_id(1), listensocket(-1), running(false), wakeup_pipe(BinaryFDStream::PIPE) {
}

NonblockingServer::~NonblockingServer() {
	if (listensocket) {
		close(listensocket);
		listensocket = -1;
	}
	stopAllWorkers();
}

void NonblockingServer::readNB(Connection &c) {
	try {
		auto is_eof = c.stream->readNB(*(c.readbuffer), true);
		if (is_eof) {
			c.close();
			return;
		}
	}
	catch (const std::exception &e) {
		Log::error("%d: Exception during readNB: %s", c.id, e.what());
		c.close();
		return;
	}

	if (c.readbuffer->isRead()) {
		try {
			c.startProcessing();
		}
		catch (const std::exception &e) {
			Log::error("%d: Exception when processing command: %s", c.id, e.what());
			c.close();
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
		c.close();
		return;
	}
}

/*
 * job-queue
 */
void NonblockingServer::enqueueTask(Connection *connection) {
	std::unique_lock<std::mutex> lock(job_queue_mutex);
	try {
		job_queue.push(connection);
	}
	catch (...) {
		connection->close();
		throw;
	}
	connection->state = Connection::State::PROCESSING_DATA_ASYNC;
	lock.unlock();
	job_queue_cond.notify_one();
}

// Waits for a task and returns it. If no further tasks are availabe, a nullptr is returned.
NonblockingServer::Connection *NonblockingServer::popTask() {
	std::unique_lock<std::mutex> lock(job_queue_mutex);
	while (true) {
		while (running && job_queue.empty()) {
			job_queue_cond.wait(lock);
		}
		if (!running)
			return nullptr;
		auto connection = job_queue.front();
		job_queue.pop();
		if (connection->is_closed) {
			// it's possible that the connection had a problem somewhere..
			// We don't want to spend work on it, but we need to pass ownership back to the main thread so it can be reaped.
			connection->state = Connection::State::IDLE;
			continue;
		}
		if (connection->state != Connection::State::PROCESSING_DATA_ASYNC)
			throw MustNotHappenException("NonblockingServer: popped a connection from the job queue which is not in state PROCESSING_ASYNC");
		return connection;
	}
	// dear eclipse: please stop complaining
	return nullptr;
}


NonblockingServer::Connection *NonblockingServer::getIdleConnectionById(int id) {
	std::lock_guard<std::recursive_mutex> connections_lock(connections_mutex);
	for (auto &c : connections) {
		if (c->id == id && c->state == Connection::State::IDLE)
			return c.get();
	}
	throw ArgumentException("No idle connection with the given ID found");
}


void NonblockingServer::setWorkerThreads(int num_workers) {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call setWorkerThreads() after start()");

	this->num_workers = num_workers;
}

void NonblockingServer::listen(int portnr) {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call listen() after start()");

	listensocket = getListeningSocket(portnr);
}

void NonblockingServer::worker_thread() {
	try {
		while (true) {
			auto connection = popTask();
			if (connection == nullptr)
				return;
			connection->processDataAsync();
			// as soon as this method call returns, the main thread may or may not have deleted the connection,
			// which means that unfortunately we cannot safely check this.
			//if (connection->state == Connection::State::PROCESSING_DATA_ASYNC)
			//	throw MustNotHappenException("processData() did not change the state, expected PROCESS_ASYNC, IDLE or a reply");
		}
	}
	catch (const std::exception &e) {
		Log::error("NonblockingServer: Worker thread terminated with exception %s", e.what());
	}
	Log::info("worker thread stopping..");
}

void NonblockingServer::start() {
	if (!listensocket)
		throw ArgumentException("NonblockingServer: call listen() before start()");
	bool expected = false;
	if (!running.compare_exchange_strong(expected, true))
		throw ArgumentException("NonblockingServer: already running");

	for (int i=0;i<num_workers;i++)
		workers.emplace_back(&NonblockingServer::worker_thread, this);

	while (true) {
		struct timeval tv{60,0};
		fd_set readfds;
		fd_set writefds;
		int maxfd = 0;

		FD_ZERO(&readfds);
		FD_ZERO(&writefds);
		std::unique_lock<std::recursive_mutex> connections_lock(connections_mutex);
		auto it = connections.begin();
		while (it != connections.end()) {
			auto &c = *it;
			int fd = c->fd;
			Connection::State state = c->state;
			if (c->is_closed) {
				if (state != Connection::State::PROCESSING_DATA_ASYNC) {
					auto id = c->id;
					it = connections.erase(it);
					Log::info("%d: closing, %lu clients remain", id, connections.size());
					continue; // avoid the ++it
				}
			}
			else {
				if (state == Connection::State::WRITING_DATA)
					FD_SET(fd, &writefds);
				else if (state == Connection::State::READING_DATA)
					FD_SET(fd, &readfds);
				else {
					++it;
					continue;
				}
			}
			maxfd = std::max(fd, maxfd);
			++it;
		}
		connections_lock.unlock();

		maxfd = std::max(wakeup_pipe.getReadFD(), maxfd);
		FD_SET(wakeup_pipe.getReadFD(), &readfds);

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

		if (!running) {
			Log::info("Stopping Server");
			break;
		}

		if (FD_ISSET(wakeup_pipe.getReadFD(), &readfds)) {
			// we have been woken, now we need to read any outstanding data or the pipe will remain readable
			char buf[1024];
			read(wakeup_pipe.getReadFD(), buf, 1024);
		}

		connections_lock.lock();
		for (auto &c : connections) {
			Connection::State state = c->state;
			if (state == Connection::State::WRITING_DATA && FD_ISSET(c->fd, &writefds)) {
				writeNB(*c);
			}
			else if (state == Connection::State::READING_DATA && FD_ISSET(c->fd, &readfds)) {
				readNB(*c);
			}
		}
		connections_lock.unlock();

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
			connections_lock.lock();
			connections.push_back( createConnection(new_fd, next_id++) );
			connections_lock.unlock();
		}
	}

	stopAllWorkers();
}

void NonblockingServer::stopAllWorkers() {
	if (!workers.empty()) {
		running = false;
		job_queue_cond.notify_all();
		for (size_t i=0;i<workers.size();i++)
			workers[i].join();
		workers.clear();
	}
}

void NonblockingServer::wake() {
	// if the loop is currently in a select() phase, this write will wake it up.
	char c = 0;
	wakeup_pipe.write(c);
}

void NonblockingServer::stop() {
	Log::info("Sending signal to stop server");
	running = false;
	wake();
}
