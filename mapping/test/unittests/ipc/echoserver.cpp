#include "util/server_nonblocking.h"
#include "util/make_unique.h"
#include "util/log.h"

#include <gtest/gtest.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <algorithm> // std::min
#include <atomic>


// Unit tests are supposed to be quick. If you want to really stress-test the server,
// increase these values to something like 10/50.
static const int NUM_CLIENTS = 3;
static const int NUM_REQUESTS = 3;

static const int SERVER_PORT = 51234;
static const size_t SERVER_BUFFER_SIZE = 65536;

// to see minimum/default/maximum buffer sizes, do:
// cat /proc/sys/net/ipv4/tcp_{r,w}mem
// 4096    87380   6291456
// 4096    16384   4194304
// so on this machine, our buffers should be >6 MB to make sure we have partial reads and writes
static size_t PACKET_SIZE = 6291457; // 6 MB + 1 Byte



/*
 * First, create an EchoServer, which writes back anything the client sends.
 */
class EchoServerConnection : public NonblockingServer::Connection {
	public:
		EchoServerConnection(NonblockingServer &server, int fd, int id);
		~EchoServerConnection();
	private:
		virtual void processData(std::unique_ptr<BinaryReadBuffer> request);
};

EchoServerConnection::EchoServerConnection(NonblockingServer &server, int fd, int id) : Connection(server, fd, id) {
}

EchoServerConnection::~EchoServerConnection() {
}

void EchoServerConnection::processData(std::unique_ptr<BinaryReadBuffer> request) {
	auto response = make_unique<BinaryWriteBuffer>();
	size_t bytes_read = 0, bytes_total = request->getPayloadSize();
	char buffer[SERVER_BUFFER_SIZE];
	while (bytes_read < bytes_total) {
		size_t remaining = bytes_total - bytes_read;
		size_t next_batch_size = std::min(SERVER_BUFFER_SIZE, remaining);
		request->read(buffer, next_batch_size);
		response->write(buffer, next_batch_size);
		bytes_read += next_batch_size;
	}

	startWritingData(std::move(response));
}


class EchoServer : public NonblockingServer {
	public:
		using NonblockingServer::NonblockingServer;
		virtual ~EchoServer() {};
	private:
		virtual std::unique_ptr<Connection> createConnection(int fd, int id);
};

std::unique_ptr<NonblockingServer::Connection> EchoServer::createConnection(int fd, int id) {
	return make_unique<EchoServerConnection>(*this, fd, id);
}

/*
 * The server is running in its own thread, so we need to synchronize initialisation with the main thread
 */
static std::mutex server_initialization_mutex;
static std::atomic<bool> server_thread_failed;
static std::unique_ptr<EchoServer> server;

static void run_server() {
	try {
		auto portnr = SERVER_PORT;

		server_initialization_mutex.lock();

		server = make_unique<EchoServer>();
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
 * The clients are running in their own threads, so they must not throw exceptions (that would terminat() the program).
 * Instead, they'll print errors to stdout and report failure by setting this boolean to false.
 */
static std::atomic<bool> all_clients_successful;

/*
 * Generate a random packet
 */
std::string getRandomString() {
	size_t size = PACKET_SIZE;
	std::string random;
	random.reserve(size);
	for (size_t i=0;i<size;i++) {
		char c = (i*7)%255;
		random += c;
	}
	if (random.size() != size)
		printf("Wrong string size, expected %lu, got %lu\n", size, random.size());
	return random;
}

static void run_client(int id) {
	int req=0;
	try {
		auto stream = make_unique<BinaryFDStream>("127.0.0.1", SERVER_PORT, true);

		const auto request_bytes = getRandomString();

		for (req=0;req<NUM_REQUESTS;req++) {
			//printf("Client %d is at %d of %d\n", id, req, NUM_REQUESTS);
			BinaryWriteBuffer request;
			request.enableLinking();
			request.write(request_bytes.c_str(), request_bytes.size(), true);
			stream->write(request);

			BinaryReadBuffer response;
			stream->read(response);

			for (size_t pos=0;pos < request_bytes.size(); pos++) {
				auto byte = response.read<char>();
				if (byte != request_bytes[pos]) {
					printf("Error in client %d, got mismatching bytes on request %d of %d at position %d\n", id, req, NUM_REQUESTS, pos);
					all_clients_successful = false;
				}
			}
		}
	}
	catch (const std::exception &e) {
		printf("Client %d aborted with an exception in request %d of %d: %s\n", req, NUM_REQUESTS, id, e.what());
		all_clients_successful = false;
	}
}


// Spawn a server and hammer it with concurrent requests. See if communication fails somewhere.
TEST(NonblockingServer, EchoServer) {
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
