#ifndef UTIL_SERVER_NONBLOCKING_H
#define UTIL_SERVER_NONBLOCKING_H

#include "util/binarystream.h"

#include <memory>

/*
 * A single-threaded server based on non-blocking network IO.
 *
 * TODO: allocate worker threads for command dispatch
 * TODO: allow multiple listen sockets (ipv4, ipv6, af_unix, multiple interfaces, ...)
 */
class NonblockingServer {
	public:
		class Connection {
			friend class NonblockingServer;
			protected:
				Connection(int fd, int id);
			public:
				virtual ~Connection();
			private:
				virtual std::unique_ptr<BinaryWriteBuffer> processRequest(NonblockingServer &server, std::unique_ptr<BinaryReadBuffer> request) = 0;

				std::unique_ptr<BinaryFDStream> stream;
				std::unique_ptr<BinaryWriteBuffer> writebuffer;
				std::unique_ptr<BinaryReadBuffer> readbuffer;
			protected:
				const int fd;
				const int id;
		};

		NonblockingServer();
		virtual ~NonblockingServer();
		void start(int portnr);
	private:
		bool readNB(Connection &connection);
		void writeNB(Connection &connection);

		virtual std::unique_ptr<Connection> createConnection(int fd, int id) = 0;
		int next_id;
		int listensocket;
		std::vector<std::unique_ptr<Connection>> connections;
};



#endif
