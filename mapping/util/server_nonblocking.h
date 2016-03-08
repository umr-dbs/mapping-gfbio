#ifndef UTIL_SERVER_NONBLOCKING_H
#define UTIL_SERVER_NONBLOCKING_H

#include "util/binarystream.h"

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <memory>
#include <atomic>
#include <condition_variable>


/*
 * A server based on non-blocking network IO.
 *
 * TODO: allow multiple listen sockets (ipv4, ipv6, af_unix, multiple interfaces, ...)
 */
class NonblockingServer {
	public:
		class Connection {
			friend class NonblockingServer;
			protected:
				Connection(NonblockingServer &server, int fd, int id);
			public:
				virtual ~Connection();
			private:
				virtual void processData(std::unique_ptr<BinaryReadBuffer> request) = 0;
				virtual void processDataAsync();

				const int fd;
				std::unique_ptr<BinaryFDStream> stream;
				std::unique_ptr<BinaryWriteBuffer> writebuffer;
				std::unique_ptr<BinaryReadBuffer> readbuffer;

				/*
				 * The basic premise of a connection is that each side takes turns sending exactly one packet of data, starting with the client.
				 *
				 * This state does not only track whose turn it is to send data, it also acts as a locking primitive, documenting which
				 * thread or object may modify the connection:
				 * - during READING and WRITING, the Server modifies the buffers.
				 * - during PROCESSING, the connection got a packet and processes it in the main thread.
				 * - during PROCESSING_ASYNC, the connection is working in a worker thread.
				 * - during IDLE, it is the connection's turn to send a packet, but it is waiting for something.
				 */
				enum class State {
					INITIALIZING,
					READING_DATA,
					PROCESSING_DATA,
					PROCESSING_DATA_ASYNC,
					WRITING_DATA,
					IDLE
				};
				std::atomic<State> state;
				std::atomic<bool> is_closed;

				// The following methods all model state changes. Private methods are called by the Server, protected by the Connection.
				void startProcessing();
				void waitForData();
			protected:
				void startWritingData(std::unique_ptr<BinaryWriteBuffer> writebuffer);
				void enqueueForAsyncProcessing();
				void goIdle();

				void close();

				NonblockingServer &server;
				const int id;
		};

		NonblockingServer();
		virtual ~NonblockingServer();
		/*
		 * Sets up the listening socket, but does not accept any connections yet.
		 */
		void listen(int portnr);
		/*
		 * Sets the number of worker threads to use. Defaults to 0, which forbids asynchronous processing.
		 * Set this before calling start();
		 */
		void setWorkerThreads(int num_workers);
		/*
		 * After listen() succeeded, start the main loop
		 */
		void start();
		/*
		 * Stop the server. As start() does not terminate, this must be called from a separate thread.
		 */
		void stop();
		/*
		 * Gets a reference to a connection by id. Only IDLE connections are returned.
		 */
		Connection *getIdleConnectionById(int id);
		/*
		 * Wake the server up, interrupting a select() call. Used to notify the server about
		 * changes in the connections or about stopping.
		 */
		void wake();
	private:
		void readNB(Connection &connection);
		void writeNB(Connection &connection);

		virtual std::unique_ptr<Connection> createConnection(int fd, int id) = 0;

		// Workers
		int num_workers;
		std::vector<std::thread> workers;
		void worker_thread();
		void stopAllWorkers();

		// job-queue
		std::queue<Connection *> job_queue;
		std::mutex job_queue_mutex;
		std::condition_variable job_queue_cond;
		void enqueueTask(Connection *connection);
		Connection *popTask();

		// other things
		int next_id;
		int listensocket;
		std::vector<std::unique_ptr<Connection>> connections;
		std::recursive_mutex connections_mutex;

		std::atomic<bool> running;
		BinaryFDStream wakeup_pipe;
};



#endif
