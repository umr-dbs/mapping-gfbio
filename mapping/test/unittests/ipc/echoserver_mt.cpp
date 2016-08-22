#include "util/server_nonblocking.h"
#include "util/make_unique.h"
#include "util/log.h"

#include <gtest/gtest.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <algorithm> // std::min
#include <atomic>


/*
 * This is an echo-server with worker threads.
 *
 * Unlike the other echo-server, these clients will only send integers, not huge buffers.
 * All even numbers will be echoed directly, all odd numbers will be echoed by the worker threads.
 */

static const int NUM_CLIENTS = 3;
static const int NUM_REQUESTS = 500;

static const int SERVER_PORT = 51236;
static const int SERVER_WORKER_THREADS = 3;


/*
 * First, create an MTEchoServer, which writes back anything the client sends.
 */
class MTEchoServerConnection : public NonblockingServer::Connection {
	public:
		MTEchoServerConnection(NonblockingServer &server, int fd, int id);
		~MTEchoServerConnection();
	private:
		virtual void processData(std::unique_ptr<BinaryReadBuffer> request);
		virtual void processDataAsync();
		int data;
};

MTEchoServerConnection::MTEchoServerConnection(NonblockingServer &server, int fd, int id) : Connection(server, fd, id), data(0) {
}

MTEchoServerConnection::~MTEchoServerConnection() {
}

void MTEchoServerConnection::processData(std::unique_ptr<BinaryReadBuffer> request) {
	data = request->read<int>();

	if (data % 2 == 0) {
		enqueueForAsyncProcessing();
	}
	else {
		auto response = make_unique<BinaryWriteBuffer>();
		response->write(data);
		startWritingData(std::move(response));
	}
}

void MTEchoServerConnection::processDataAsync() {
	auto response = make_unique<BinaryWriteBuffer>();
	response->write(data);
	startWritingData(std::move(response));
}


class MTEchoServer : public NonblockingServer {
	public:
		using NonblockingServer::NonblockingServer;
		virtual ~MTEchoServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);
};

std::unique_ptr<NonblockingServer::Connection> MTEchoServer::createConnection(int fd, int id) {
	return make_unique<MTEchoServerConnection>(*this, fd, id);
}

/*
 * The server is running in its own thread, so we need to synchronize initialisation with the main thread
 */
static std::mutex server_initialization_mutex;
static std::atomic<bool> server_thread_failed;
static std::unique_ptr<MTEchoServer> server;

static void run_server() {
	try {
		auto portnr = SERVER_PORT;

		server_initialization_mutex.lock();

		server = make_unique<MTEchoServer>();
		server->setWorkerThreads(SERVER_WORKER_THREADS);
		server->listen(portnr);

		server_initialization_mutex.unlock();
		server->start();
		server.reset(nullptr);
	}
	catch (const std::exception &e) {
		printf("Error on server: %s\n", e.what());
		server_thread_failed = true;
	}
}


/*
 * The clients are running in their own threads, so they must not throw exceptions (that would terminate() the program).
 * Instead, they'll print errors to stdout and report failure by setting this boolean to false.
 */
static std::atomic<bool> all_clients_successful;

static void run_client(int id) {
	int req=0;
	try {
		auto stream = BinaryStream::connectTCP("127.0.0.1", SERVER_PORT, true);

		for (req=0;req<NUM_REQUESTS;req++) {
			BinaryWriteBuffer request;
			request.write((int) req);
			stream.write(request);

			BinaryReadBuffer response;
			stream.read(response);

			auto res = response.read<int>();
			if (res != req) {
				printf("Error in client %d, got mismatching number on request %d, got %d\n", id, req, res);
				all_clients_successful = false;
			}
		}
	}
	catch (const std::exception &e) {
		printf("Client %d aborted with an exception in request %d of %d: %s\n", id, req, NUM_REQUESTS, e.what());
		all_clients_successful = false;
	}
}


// Spawn a server and hammer it with concurrent requests. See if communication fails somewhere.
TEST(NonblockingServer, MTEchoServer) {
	Log::off();
	all_clients_successful = true;
	server_thread_failed = false;

	std::thread server_thread(run_server);

	// wait for the listening socket to exist
	bool server_is_running = false;
	while (!server_is_running) {
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		if (server_thread_failed) {
			ADD_FAILURE() << "Problem when initializing or running the server";
			server_thread.join();
			return;
		}
		server_initialization_mutex.lock();
		if (server != nullptr)
			server_is_running = true;
		server_initialization_mutex.unlock();
	}

	// run all clients
	std::thread client_threads[NUM_CLIENTS];

	for (int i=0;i<NUM_CLIENTS;i++)
		client_threads[i] = std::thread(run_client, i);

	// wait until all clients are done
	for (int i=0;i<NUM_CLIENTS;i++)
		client_threads[i].join();

	// stop the server
	server->stop();
	server_thread.join();

	EXPECT_EQ(true, all_clients_successful);
}
