#include "util/socket.h"
#include "raster/exceptions.h"

#include <sstream>
#include <string.h> // memset(), strerror()
#include <memory>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>


Socket::Socket(const char *server_path) : is_eof(false), read_fd(-1), write_fd(-1) {
	int new_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (new_fd < 0)
		throw PlatformException("Socket: unable to create socket()");

	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));

	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, server_path);
	if (connect(new_fd, (sockaddr *) &server_addr, sizeof(server_addr)) == -1) {
		::close(new_fd);
		throw PlatformException("Socket: unable to connect()");
	}

	read_fd = new_fd;
	write_fd = new_fd;
}
Socket::Socket(int read_fd, int write_fd) : is_eof(false), read_fd(read_fd), write_fd(write_fd) {
	if (write_fd == -2)
		write_fd = read_fd;
}

Socket::~Socket() {
	close();
}

void Socket::close() {
	if (read_fd >= 0) {
		::close(read_fd);
		if (read_fd == write_fd)
			write_fd = -1;
		read_fd = -1;
	}
	if (write_fd >= 0) {
		::close(write_fd);
		write_fd = -1;
	}
}

void Socket::write(const char *buffer, size_t len) {
	if (write_fd < 0) {
		std::ostringstream msg;
		msg << "Socket: cannot write to closed socket " << write_fd << " in pid " << getpid();
		throw PlatformException(msg.str());
	}
	//auto res = ::send(write_fd, buffer, len, MSG_NOSIGNAL);
	auto res = ::write(write_fd, buffer, len);
	if (res < 0 || (size_t) res != len) {
		std::ostringstream msg;
		msg << "Socket: write() failed: " << strerror(errno);
		throw PlatformException(msg.str());
	}

	//fprintf(stderr, "Socket: written %lu bytes\n", len);
}


size_t Socket::read(char *buffer, size_t len, bool allow_eof) {
	if (read_fd < 0) {
		std::ostringstream msg;
		msg << "Socket: cannot read from closed socket " << read_fd << " in pid " << getpid();
		throw PlatformException(msg.str());
	}
	if (is_eof)
		throw PlatformException("Socket: tried to read from a socket which is eof'ed");

	size_t remaining = len;
	size_t bytes_read = 0;
	while (remaining > 0) {
		// Note: MSG_WAITALL does not guarantee that the whole buffer is filled; the loop is still required
		//auto r = ::recv(read_fd, buffer, remaining, MSG_WAITALL);
		auto r = ::read(read_fd, buffer, remaining);
		if (r == 0) {
			is_eof = true;
			if (!allow_eof || bytes_read > 0)
				throw PlatformException("Socket: unexpected eof");
			return bytes_read;
		}
		if (r < 0) {
			std::ostringstream msg;
			msg << "Socket: read() failed: " << strerror(errno);
			throw PlatformException(msg.str());
		}
		bytes_read += r;
		buffer += r;
		remaining -= r;
	}
	if (bytes_read != len)
		throw PlatformException("Socket: invalid read");

	//fprintf(stderr, "Socket: read %lu bytes\n", bytes_read);
	return bytes_read;
}



void Socket::write(const std::string &string) {
	size_t len = string.size();
	if (len > (size_t) (1<<31))
		throw PlatformException("Socket: String too large to transmit");
	write(len);
	write(string.data(), len);
}

size_t Socket::read(std::string *string, bool allow_eof) {
	size_t len;
	if (read(&len, allow_eof) == 0)
		return 0;

	std::unique_ptr<char[]> buffer( new char[len] );
	read(buffer.get(), len);
	string->assign(buffer.get(), len);
	return len + sizeof(len);
}
