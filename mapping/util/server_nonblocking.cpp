
#include "util/exceptions.h"
#include "util/server_nonblocking.h"
#include "util/log.h"

#include <sys/types.h>

// socket() etc
#include <sys/socket.h>
#include <netdb.h>

#include <string.h>
#include <errno.h>

// tcp
#include <netinet/in.h>
#include <netinet/tcp.h>
// af_unix
#include <sys/un.h>
#include <sys/stat.h>
// waitpid
#include <sys/wait.h>


/*
 * Connection
 */
NonblockingServer::Connection::Connection(NonblockingServer &server, int fd, int id)
	: fd(fd), stream(BinaryStream::fromAcceptedSocket(fd,true)), state(State::INITIALIZING), is_closed(false), server(server), id(id) {
	stream.makeNonBlocking();
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
		throw MustNotHappenException("processData() did not change the state, expected PROCESS_ASYNC, PROCESS_FORKED, IDLE or a reply");
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
		stream.close();
	}
}

void NonblockingServer::Connection::startWritingData(std::unique_ptr<BinaryWriteBuffer> new_writebuffer) {
	State old_state = state;
	if (old_state != State::PROCESSING_DATA && old_state != State::PROCESSING_DATA_ASYNC && old_state != State::IDLE)
		throw MustNotHappenException("Connection::startWritingData() cannot be called in current state");
	readbuffer.reset(nullptr);
	writebuffer = std::move(new_writebuffer);
	auto &server = this->server;
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

void NonblockingServer::Connection::forkAndProcess(int timeout_seconds) {
	// Do not allow forking from anything but the main thread, and only while the Connection object owns the handle
	if (state != State::PROCESSING_DATA)
		throw MustNotHappenException("Connection::forkAndProcess() cannot be called in current state");
	if (!server.running || !server.allow_forking)
		throw MustNotHappenException("Connection::forkAndProcess(): server is not running or not configured for forking");

	// Do the actual forking
	pid_t pid = fork();
	if (pid < 0)
		throw PlatformException(concat("fork() failed: ", strerror(errno)));

	if (pid > 0) {
		// This is still the parent process.

		// Notify the server that a connection has fork'ed.
		server.registerForkedProcess(pid, timeout_seconds);
		// Make sure the fd is closed and the connection gets cleaned up in the main loop
		close();
		state = Connection::State::PROCESSING_DATA_FORKED;
		return;
	}
	else if (pid == 0) {
		// This is the child process
		try {
			server.cleanupAfterFork();

			// Without the coordination of the NonblockingServer, the connection and its connection API will no longer work.
			// Neither the stream nor the buffers may remain accessible.
			state = State::PROCESSING_DATA_FORKED;

			// We "steal" the stream from the connection before closing it. The child process can access the stream directly.
			BinaryStream new_stream = std::move(stream);
			new_stream.makeBlocking();
			close();

			Log::info("New child process starting");
			auto start_c = clock();
			struct timespec start_t;
			clock_gettime(CLOCK_MONOTONIC, &start_t);
			try {
				processDataForked(std::move(new_stream));
			}
			catch (const std::exception &e) {
				Log::warn("Exception in child process: %s", e.what());
			}
			auto end_c = clock();
			struct timespec end_t;
			clock_gettime(CLOCK_MONOTONIC, &end_t);

			double c = (double) (end_c - start_c) / CLOCKS_PER_SEC;
			double t = (double) (end_t.tv_sec - start_t.tv_sec) + (double) (end_t.tv_nsec - start_t.tv_nsec) / 1000000000;

			Log::info("Child process finished, %.3fs real, %.3fs CPU", t, c);
			/*
			auto p = Profiler::get();
			for (auto &s : p) {
				Log::info("%s", s.c_str());
			}
			*/
		} catch (const std::exception &e) {
			Log::error("Child process terminated with an exception: %s\n", e.what());
		} catch (...) {
			Log::error("Child process terminated with an exception\n");
		}
		exit(0); // make sure control never returns to the Server on our child process
	}
}

void NonblockingServer::Connection::processDataAsync() {
	throw MustNotHappenException("processDataAsync not implemented on this connection!");
}
void NonblockingServer::Connection::processDataForked(BinaryStream stream) {
	throw MustNotHappenException("processDataForked not implemented on this connection!");
}



/*
 * Helper Function
 */
static int getListeningSocket(int port) {
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
		throw NetworkException(concat("failed to bind to any interface on port ", port));

	if (listen(sock, SOMAXCONN) == -1)
		throw NetworkException("listen() failed");

	return sock;
}

static int getListeningSocket(const std::string &socket_path, int umode) {
	// get rid of leftover sockets
	unlink(socket_path.c_str());

	int sock;

	// create a socket
	sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock < 0)
		throw NetworkException(concat("socket() failed: ", strerror(errno)));

	// bind socket
	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));
	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, socket_path.c_str());
	if (bind(sock, (sockaddr *) &server_addr, sizeof(server_addr)) < 0)
		throw NetworkException(concat("bind() failed: ", strerror(errno)));

	chmod(socket_path.c_str(), umode);

	return sock;
}


/*
 * Nonblocking Server
 */
NonblockingServer::NonblockingServer()
	: num_workers(0), allow_forking(false), next_id(1), listensocket(-1), running(false), wakeup_pipe(BinaryStream::makePipe()) {
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
		auto is_eof = c.stream.readNB(*(c.readbuffer), true);
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
		c.stream.writeNB(*(c.writebuffer));
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
		if (c->id == id && c->state == Connection::State::IDLE && !c->is_closed)
			return c.get();
	}
	throw ArgumentException("No idle connection with the given ID found");
}

void NonblockingServer::setWorkerThreads(int num_workers) {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call setWorkerThreads() after start()");

	this->num_workers = num_workers;
}

void NonblockingServer::allowForking() {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call allowForking() after start()");

	allow_forking = true;
}

void NonblockingServer::listen(int portnr) {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call listen() after start()");
	if (listensocket >= 0)
		throw MustNotHappenException("NonblockingServer: can only listen on one port or socket at the moment");

	listensocket = getListeningSocket(portnr);
}

void NonblockingServer::listen(const std::string &socket_path, int umode) {
	if (running)
		throw MustNotHappenException("NonblockingServer: do not call listen() after start()");
	if (listensocket >= 0)
		throw MustNotHappenException("NonblockingServer: can only listen on one port or socket at the moment");

	listensocket = getListeningSocket(socket_path, umode);
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

void NonblockingServer::registerForkedProcess(pid_t pid, int timeout_seconds) {
	struct timespec timeout;
	if (timeout_seconds <= 0) {
		timeout.tv_sec = std::numeric_limits<time_t>::max();
		timeout.tv_nsec = 0;
	}
	else {
		clock_gettime(CLOCK_MONOTONIC, &timeout);
		timeout.tv_sec += timeout_seconds;
	}
	running_child_processes[pid] = timeout;
}


static int cmpTimespec(const struct timespec &t1, const struct timespec &t2) {
	if (t1.tv_sec < t2.tv_sec)
		return -1;
	if (t1.tv_sec > t2.tv_sec)
		return 1;
	if (t1.tv_nsec < t2.tv_nsec)
		return -1;
	if (t1.tv_nsec > t2.tv_nsec)
		return 1;
	return 0;
}

void NonblockingServer::reapAllChildProcesses(bool force_timeout) {
	if (!allow_forking)
		return;
	// try to reap our children
	int status;
	pid_t exited_pid;
	while ((exited_pid = waitpid(-1, &status, WNOHANG)) > 0) {
		Log::info("Child process %d no longer exists", (int) exited_pid);
		running_child_processes.erase(exited_pid);
	}
	// Kill all overdue children
	struct timespec current_t;
	clock_gettime(CLOCK_MONOTONIC, &current_t);

	for (auto it = running_child_processes.begin(); it != running_child_processes.end(); ) {
		auto timeout_t = it->second;
		if (force_timeout || cmpTimespec(timeout_t, current_t) < 0) {
			auto timeouted_pid = it->first;
			Log::warn("Child process %d gets killed due to timeout", (int) timeouted_pid);

			if (kill(timeouted_pid, SIGHUP) < 0) { // TODO: SIGKILL?
				Log::error("kill() failed: %s", strerror(errno));
			}
			// the postincrement of the iterator is important to avoid using an invalid iterator
			running_child_processes.erase(it++);
		}
		else {
			++it;
		}
	}
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
		reapAllChildProcesses();

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
    		//struct sockaddr_un remote_addr; // for AF_UNIX
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

			connections_lock.lock();
			connections.push_back( createConnection(new_fd, next_id++) );
			connections_lock.unlock();
		}
	}

	stopAllWorkers();
	reapAllChildProcesses(true);
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
	write(wakeup_pipe.getWriteFD(), &c, 1);
}

void NonblockingServer::stop() {
	Log::info("Sending signal to stop server");
	running = false;
	wake();
}

void NonblockingServer::cleanupAfterFork() {
	// TODO: implement.
	// open fds don't hurt, but they really shouldn't stick around.
}
