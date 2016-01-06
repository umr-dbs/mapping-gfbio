#include "util/exceptions.h"
#include "rasterdb/backend.h"
#include "rasterdb/backend_local.h"
#include "rasterdb/backend_remote.h"
#include "util/binarystream.h"
#include "util/configuration.h"
#include "util/make_unique.h"

// socket() etc
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// select()
#include <sys/time.h>
#include <unistd.h>


#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <arpa/inet.h>
#include <sys/wait.h>


int getListeningSocket(int port, int backlog = 10) {
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
		//fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		throw NetworkException("getaddrinfo() failed");
	}

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


class Connection {
	public:
		Connection(int fd);
		int input();
		void writeNB();
		bool hasWriteBuffer() { return writebuffer != nullptr; }
		int fd;
		int id;
	private:
		std::unique_ptr<BinaryFDStream> stream;
		std::shared_ptr<LocalRasterDBBackend> backend;
		std::unique_ptr<BinaryWriteBuffer> writebuffer;
};

static int connection_id = 1;
Connection::Connection(int fd) : fd(fd) {
	id = connection_id++;
	stream.reset( new BinaryFDStream(fd,fd) );
	printf("%d: connected\n", id);
	backend = make_unique<LocalRasterDBBackend>();
}

int Connection::input() {
	if (writebuffer != nullptr)
		return 0;

	uint8_t OK = 48;

	uint8_t c;
	if (!stream->read(&c, true)) {
		printf("%d: disconnected\n", id);
		return -1;
	}

	printf("%d: got command %d\n", id, c);


	if (backend == nullptr && c >= RemoteRasterDBBackend::FIRST_SOURCE_SPECIFIC_COMMAND)
		return -1;

	writebuffer = make_unique<BinaryWriteBuffer>();

	switch (c) {
		case RemoteRasterDBBackend::COMMAND_EXIT: {
			return -1;
			break;
		}
		case RemoteRasterDBBackend::COMMAND_ENUMERATESOURCES: {
			std::vector<std::string> sourcenames = backend->enumerateSources();

			size_t size = sourcenames.size();
			writebuffer->write(size);
			for (const auto &name : sourcenames)
				writebuffer->write(name);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READANYJSON: {
			std::string name;
			stream->read(&name);

			auto json = backend->readJSON(name);
			writebuffer->write(json);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_OPEN: {
			if (backend->isOpen())
				throw NetworkException("Cannot call open() twice!");
			std::string path;
			stream->read(&path);

			backend->open(path, false);
			writebuffer->write(OK);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READJSON: {
			auto json = backend->readJSON();
			writebuffer->write(json);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_CREATERASTER:
		//case RemoteRasterDBBackend::COMMAND_WRITETILE:
		case RemoteRasterDBBackend::COMMAND_GETCLOSESTRASTER: {
			int channelid;
			stream->read(&channelid);
			double t1, t2;
			stream->read(&t1);
			stream->read(&t2);

			try {
				auto res = backend->getClosestRaster(channelid, t1, t2);
				printf("%d: found closest raster with id %ld, time %f->%f\n", id, res.rasterid, res.time_start, res.time_end);
				writebuffer->write(res);
			}
			catch (const SourceException &e) {
				RasterDBBackend::RasterDescription r(-1, 0, 0);
				writebuffer->write(r);
				std::string error(e.what());
				writebuffer->write(error);
			}
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READATTRIBUTES: {
			RasterDBBackend::rasterid_t rasterid;
			stream->read(&rasterid);
			AttributeMaps attributes;
			backend->readAttributes(rasterid, attributes);

			std::string empty("");
			for (auto pair : attributes.textual()) {
				writebuffer->write(pair.first);
				writebuffer->write(pair.second);
			}
			writebuffer->write(empty);
			for (auto pair : attributes.numeric()) {
				writebuffer->write(pair.first);
				writebuffer->write(pair.second);
			}
			writebuffer->write(empty);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_GETBESTZOOM: {
			RasterDBBackend::rasterid_t rasterid;
			stream->read(&rasterid);
			int desiredzoom;
			stream->read(&desiredzoom);

			int bestzoom = backend->getBestZoom(rasterid, desiredzoom);
			writebuffer->write(bestzoom);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_ENUMERATETILES: {
			int channelid;
			stream->read(&channelid);
			RemoteRasterDBBackend::rasterid_t rasterid;
			stream->read(&rasterid);
			int x1, y1, x2, y2, zoom;
			stream->read(&x1);
			stream->read(&y1);
			stream->read(&x2);
			stream->read(&y2);
			stream->read(&zoom);

			auto res = backend->enumerateTiles(channelid, rasterid, x1, y1, x2, y2, zoom);
			size_t size = res.size();
			printf("%d: (%d,%d) -> (%d,%d), channel %d, raster %ld at zoom %d yielded %lu tiles\n", id, x1, y1, x2, y2, channelid, rasterid, zoom, size);
			writebuffer->write(size);
			for (auto &td : res)
				writebuffer->write(td);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_HASTILE:
		case RemoteRasterDBBackend::COMMAND_READTILE: {
			RasterDBBackend::TileDescription tile(*stream);
			printf("%d: returning tile, offset %lu, size %lu\n", id, tile.offset, tile.size);

			// To avoid copying the bytebuffer with the tiledata, we're linking the data in the writebuffer.
			// To do this, our bytebuffer's lifetime must be managed by the writebuffer.
			// Unfortunately, we have to replace our freshly allocated writebuffer with a different one right here.
			BinaryWriteBufferWithObject<ByteBuffer> *response = new BinaryWriteBufferWithObject<ByteBuffer>();
			writebuffer.reset(response);
			response->object = backend->readTile(tile);
			response->write(response->object->size);
			response->enableLinking();
			response->write((const char *) response->object->data, response->object->size);
			response->disableLinking();
			printf("%d: data sent\n", id);
			break;
		}
		default:
			return -1;
	}
	writebuffer->prepareForWriting();
	printf("%d: response of %d bytes\n", id, (int) writebuffer->getSize());
	if (writebuffer->getSize() == 0)
		writebuffer.reset(nullptr);
	return 0;
}

void Connection::writeNB() {
	if (!hasWriteBuffer())
		return;
	stream->writeNB(*writebuffer);
	if (writebuffer->isFinished()) {
		printf("%d: response sent\n", id);
		writebuffer.reset(nullptr);
	}
}


static std::vector<Connection> connections;

int main(void) {
	Configuration::loadFromDefaultPaths();

	auto portstr = Configuration::get("rasterdb.tileserver.port");
	auto portnr = atoi(portstr.c_str());
	int listensocket = getListeningSocket(portnr);

    printf("server: listening on port %d\n", portnr);

    while (true) {
        struct timeval tv{60,0};
        fd_set readfds;
        fd_set writefds;
        int maxfd = 0;

        FD_ZERO(&readfds);
        for (auto &c : connections) {
        	int fd = c.fd;
    		maxfd = std::max(fd, maxfd);
        	if (c.hasWriteBuffer()) {
				FD_SET(fd, &writefds);
        	}
        	else {
				FD_SET(fd, &readfds);
        	}
        }

    	maxfd = std::max(listensocket, maxfd);
    	FD_SET(listensocket, &readfds);

        select(maxfd+1, &readfds, &writefds, nullptr, &tv);

        auto it = connections.begin();
        while (it != connections.end()) {
        	auto &c = *it;
    		bool needs_closing = false;
    		if (c.hasWriteBuffer() && FD_ISSET(c.fd, &writefds)) {
    			c.writeNB();
    		}
    		else if (FD_ISSET(c.fd, &readfds)) {
        		try {
        			if (c.input() < 0)
        				needs_closing = true;
        		}
        		catch (const std::exception &e) {
        			fprintf(stderr, "%d: Exception: %s\n", c.id, e.what());
        			needs_closing = true;
        		}
        	}
    		if (needs_closing) {
    			auto id = c.id;
    			it = connections.erase(it);
    			printf("%d: closing, %lu clients remain\n", id, connections.size());
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
			connections.push_back( Connection(new_fd) );
        }
    }

    return 0;
}
