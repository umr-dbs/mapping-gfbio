#include "raster/exceptions.h"
#include "rasterdb/backend.h"
#include "rasterdb/backend_local.h"
#include "rasterdb/backend_remote.h"
#include "util/binarystream.h"
#include "util/configuration.h"


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

#include <arpa/inet.h>
#include <sys/wait.h>


int getListeningSocket(int port, int backlog = 50) {
	int sock;
	struct addrinfo hints, *servinfo, *p;
	int yes = 1;
	int rv;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	char portstr[16];
	snprintf(portstr, 16, "%d", port);
	if ((rv = getaddrinfo(nullptr, portstr, &hints, &servinfo)) != 0) {
		//fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		throw NetworkException("getaddrinfo() failed");
	}

	// loop through all the results and bind to the first we can
	for (p = servinfo; p != nullptr; p = p->ai_next) {
		if ((sock = socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK, p->ai_protocol)) == -1) {
			//perror("server: socket");
			continue;
		}

		// TODO: what does this do?
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
				== -1) {
			//perror("setsockopt");
			//exit(1);
			freeaddrinfo(servinfo); // all done with this structure
			throw NetworkException("setsockopt() failed");
		}

		if (bind(sock, p->ai_addr, p->ai_addrlen) == -1) {
			close(sock);
			//perror("server: bind");
			continue;
		}

		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (p == nullptr)
		throw NetworkException("failed to bind");

	if (listen(sock, backlog) == -1) {
		//perror("listen");
		throw NetworkException("listen() failed");
	}
	return sock;
}



class Connection {
	public:
		Connection(int fd);
		int input();
		int fd;
	private:
		std::unique_ptr<BinaryStream> stream;
		std::shared_ptr<LocalRasterDBBackend> backend;
};

Connection::Connection(int fd) : fd(fd) {
	stream.reset( new UnixSocket(fd,fd) );
}

int Connection::input() {
	uint8_t OK = 48;

	uint8_t c;
	if (!stream->read(&c, true)) {
		printf("fd %d disconnected\n", fd);
		return -1;
	}

	printf("got command %d on fd %d.\n", c, fd);


	if (backend.get() == nullptr && (c != RemoteRasterDBBackend::COMMAND_EXIT && c != RemoteRasterDBBackend::COMMAND_OPEN))
		return -1;

	switch (c) {
		case RemoteRasterDBBackend::COMMAND_EXIT: {
			return -1;
			break;
		}
		case RemoteRasterDBBackend::COMMAND_OPEN: {
			if (backend.get() != nullptr)
				throw NetworkException("Cannot call open() twice!");
			std::string path;
			stream->read(&path);
			backend.reset( new LocalRasterDBBackend(path.c_str(), false) );
			stream->write(OK);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READJSON: {
			auto json = backend->readJSON();
			stream->write(json);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_CREATERASTER:
		//case RemoteRasterDBBackend::COMMAND_WRITETILE:
		case RemoteRasterDBBackend::COMMAND_GETCLOSESTRASTER: {
			int channelid;
			stream->read(&channelid);
			double timestamp;
			stream->read(&timestamp);
			auto res = backend->getClosestRaster(channelid, timestamp);
			printf("returning raster with id %ld, time %f->%f\n", res.rasterid, res.time_start, res.time_end);
			stream->write(res);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_READATTRIBUTES: {
			RasterDBBackend::rasterid rasterid;
			stream->read(&rasterid);
			DirectMetadata<std::string> md_string;
			DirectMetadata<double> md_value;
			backend->readAttributes(rasterid, md_string, md_value);
			std::string empty("");
			for (auto pair : md_string) {
				stream->write(pair.first);
				stream->write(pair.second);
			}
			stream->write(empty);
			for (auto pair : md_value) {
				stream->write(pair.first);
				stream->write(pair.second);
			}
			stream->write(empty);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_GETBESTZOOM: {
			RasterDBBackend::rasterid rasterid;
			stream->read(&rasterid);
			int desiredzoom;
			stream->read(&desiredzoom);
			int bestzoom = backend->getBestZoom(rasterid, desiredzoom);
			stream->write(bestzoom);
			break;
		}
		case RemoteRasterDBBackend::COMMAND_ENUMERATETILES: {
			int channelid;
			stream->read(&channelid);
			RemoteRasterDBBackend::rasterid rasterid;
			stream->read(&rasterid);
			int x1, y1, x2, y2, zoom;
			stream->read(&x1);
			stream->read(&y1);
			stream->read(&x2);
			stream->read(&y2);
			stream->read(&zoom);
			auto res = backend->enumerateTiles(channelid, rasterid, x1, y1, x2, y2, zoom);
			size_t size = res.size();
			printf("(%d,%d) -> (%d,%d), channel %d, raster %ld at zoom %d yielded %lu tiles\n", x1, y1, x2, y2, channelid, rasterid, zoom, size);
			stream->write(size);
			for (auto &td : res)
				stream->write(td);
			break;
		}
		//case RemoteRasterDBBackend::COMMAND_HASTILE:
		case RemoteRasterDBBackend::COMMAND_READTILE: {
			RasterDBBackend::TileDescription tile(*stream);
			printf("reading tile, offset %lu, size %lu\n", tile.offset, tile.size);
			auto buffer = backend->readTile(tile);
			printf("buffer is %lu bytes\n", buffer->size);
			stream->write(buffer->size);
			printf("size sent\n");
			stream->write((const char *) buffer->data, buffer->size);
			printf("data sent\n");
			break;
		}
		default:
			return -1;
	}
	return 0;
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
        int maxfd = 0;

        FD_ZERO(&readfds);
        for (auto &c : connections) {
        	int fd = c.fd;
        	maxfd = std::max(fd, maxfd);
        	FD_SET(fd, &readfds);
        }

    	maxfd = std::max(listensocket, maxfd);
    	FD_SET(listensocket, &readfds);

    	printf("select.. ");
        select(maxfd+1, &readfds, nullptr, nullptr, &tv);
        printf("finished\n");

        auto it = connections.begin();
        while (it != connections.end()) {
        	auto &c = *it;
    		bool needs_closing = false;
        	if (FD_ISSET(c.fd, &readfds)) {
        		try {
        			if (c.input() < 0)
        				needs_closing = true;
        		}
        		catch (const std::exception &e) {
        			fprintf(stderr, "Exception in connection: %s\n", e.what());
        			needs_closing = true;
        		}
        	}
    		if (needs_closing) {
    			printf("closing connection %d\n", c.fd);
    			it = connections.erase(it);
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

			connections.push_back( Connection(new_fd) );
        }
    }

    return 0;
}
