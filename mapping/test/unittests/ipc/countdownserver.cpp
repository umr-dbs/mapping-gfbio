#include "util/server_nonblocking.h"
#include "util/make_unique.h"
#include "util/log.h"

#include <gtest/gtest.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <algorithm> // std::min
#include <atomic>


static const int SERVER_PORT = 51235;

static const int NUM_CLIENTS = 4;
static size_t START_NUMBER = 50;


/*
 * First, create a CountdownServerConnection.
 * It receives a number and sends it to a different client, until a 0 is received.
 */
class CountdownServerConnection : public NonblockingServer::Connection {
	public:
		CountdownServerConnection(NonblockingServer &server, int fd, int id);
		~CountdownServerConnection();
	private:
		virtual void processData(std::unique_ptr<BinaryReadBuffer> request);
};

CountdownServerConnection::CountdownServerConnection(NonblockingServer &server, int fd, int id) : Connection(server, fd, id) {
}

CountdownServerConnection::~CountdownServerConnection() {
}

void CountdownServerConnection::processData(std::unique_ptr<BinaryReadBuffer> request) {
	auto number = request->read<int>();

	//printf("got %d on %d\n", number, id);
	if (number >= 0) {
		auto response = make_unique<BinaryWriteBuffer>();
		response->write(number);

		// send to next connection
		// This is a hack relying on sequentially increasing IDs
		auto next_id = (id % NUM_CLIENTS) + 1;
		auto other = dynamic_cast<CountdownServerConnection *>(server.getIdleConnectionById(next_id));
		other->startWritingData(std::move(response));
	}
	goIdle();
}


class CountdownServer : public NonblockingServer {
	public:
		using NonblockingServer::NonblockingServer;
		virtual ~CountdownServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);
};

std::unique_ptr<NonblockingServer::Connection> CountdownServer::createConnection(int fd, int id) {
	return make_unique<CountdownServerConnection>(*this, fd, id);
}

/*
 * The server is running in its own thread, so we need to synchronize initialisation with the main thread
 */
static std::mutex server_initialization_mutex;
static std::atomic<bool> server_thread_failed;
static std::unique_ptr<CountdownServer> server;

static void run_server() {
	try {
		auto portnr = SERVER_PORT;

		server_initialization_mutex.lock();

		server = make_unique<CountdownServer>();
		server->listen(portnr);

		server_initialization_mutex.unlock();
		server->start();
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
static std::atomic<int> clients_handshaked{0};

static void run_client(int id, int start_number) {
	try {
		auto stream = make_unique<BinaryFDStream>("127.0.0.1", SERVER_PORT, true);

		// send initial number
		BinaryWriteBuffer request;
		request.write(start_number);
		stream->write(request);

		clients_handshaked++;

		while (true) {
			// Receive number
			BinaryReadBuffer response;
			stream->read(response);
			auto number = response.read<int>();

			// Decrement
			if (number > 0)
				number--;

			// Send back
			BinaryWriteBuffer request;
			request.write(number);
			stream->write(request);

			if (number <= 0)
				break;
		}
	}
	catch (const std::exception &e) {
		printf("Client %d aborted with an exception: %s\n", id, e.what());
		all_clients_successful = false;
	}
}


// Spawn a server and hammer it with concurrent requests. See if communication fails somewhere.
TEST(NonblockingServer, CountdownServer) {
	//Log::setLogFd(stdout);
	Log::setLevel("off");
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

	for (int i=0;i<NUM_CLIENTS-1;i++) {
		client_threads[i] = std::thread(run_client, i, -1);
	}
	// We require that all clients are connected before sending our initial number.
	while (clients_handshaked < NUM_CLIENTS-1)
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	client_threads[NUM_CLIENTS-1] = std::thread(run_client, NUM_CLIENTS-1, START_NUMBER);

	// wait until all clients are done
	for (int i=0;i<NUM_CLIENTS;i++)
		client_threads[i].join();

	// stop the server
	server->stop();
	server_thread.join();
	server.reset(nullptr);

	EXPECT_EQ(true, all_clients_successful);
}
