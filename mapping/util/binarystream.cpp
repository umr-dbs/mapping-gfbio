#include "util/binarystream.h"
#include "util/exceptions.h"

#include <string.h> // memset(), strerror()
#include <errno.h>
#include <memory>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>


BinaryStream::BinaryStream() {
}

BinaryStream::~BinaryStream() {
}

void BinaryStream::write(const std::string &string) {
	size_t len = string.size();
	if (len > (size_t) (1<<31))
		throw NetworkException("BinaryStream: String too large to transmit");
	write(len);
	write(string.data(), len);
}

void BinaryStream::flush() {

}

size_t BinaryStream::read(std::string *string, bool allow_eof) {
	size_t len;
	if (read(&len, allow_eof) == 0)
		return 0;

	std::unique_ptr<char[]> buffer( new char[len] );
	read(buffer.get(), len);
	string->assign(buffer.get(), len);
	return len + sizeof(len);
}


void BinaryStream::write(BinaryWriteBuffer &buffer) {
	if (!buffer.isWriting())
		throw ArgumentException("cannot write() a BinaryWriteBuffer when not prepared for writing");
	if (buffer.size_sent != 0)
		throw ArgumentException("cannot partially write() a BinaryWriteBuffer");

	for (size_t i=0;i<buffer.areas.size();i++)
		write(buffer.areas[i].start, buffer.areas[i].len);
	buffer.status = BinaryWriteBuffer::Status::FINISHED;
}



/*
 * BinaryFDStream
 */
BinaryFDStream::BinaryFDStream(const char *server_path) : is_eof(false), read_fd(-1), write_fd(-1) {
	int new_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (new_fd < 0)
		throw NetworkException("BinaryFDStream: unable to create socket()");

	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));

	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, server_path);
	if (connect(new_fd, (sockaddr *) &server_addr, sizeof(server_addr)) == -1) {
		::close(new_fd);
		throw NetworkException("BinaryFDStream: unable to connect()");
	}

	read_fd = new_fd;
	write_fd = new_fd;
}


BinaryFDStream::BinaryFDStream(const char *hostname, int port, bool no_delay) : is_eof(false), read_fd(-1), write_fd(-1) {
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	struct addrinfo *servinfo;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	char portstr[16];
	snprintf(portstr, 16, "%d", port);
	int status = getaddrinfo(hostname, portstr, &hints, &servinfo);
	if (status != 0)
	    throw NetworkException(concat("getaddrinfo() failed: ", gai_strerror(status)));

	int new_fd = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
	if (new_fd < 0) {
		freeaddrinfo(servinfo);
		throw NetworkException("BinaryFDStream: unable to create socket()");
	}

	if (connect(new_fd, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
		freeaddrinfo(servinfo);
		::close(new_fd);
		throw NetworkException(concat("BinaryFDStream: unable to connect(", hostname, ":", port, "/", portstr, "): ", strerror(errno)));
	}

	if ( no_delay ) {
		int one = 1;
		setsockopt(new_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
	}

	read_fd = new_fd;
	write_fd = new_fd;
	freeaddrinfo(servinfo);
}


BinaryFDStream::BinaryFDStream(int read_fd, int write_fd, bool no_delay) : is_eof(false), read_fd(read_fd), write_fd(write_fd) {
	if (write_fd == -2)
		this->write_fd = read_fd;

	if ( no_delay ) {
		int one = 1;
		setsockopt(this->write_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
	}
}

BinaryFDStream::~BinaryFDStream() {
	close();
}

void BinaryFDStream::close() {
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


void BinaryFDStream::write(const char *buffer, size_t len) {
	if (write_fd < 0) {
		throw NetworkException(concat("BinaryFDStream: cannot write to closed socket ", write_fd, " in pid ", getpid()));
	}
	//auto res = ::send(write_fd, buffer, len, MSG_NOSIGNAL);
	size_t written = 0;
	while (written < len) {
		size_t remaining = len - written;
		auto res = ::write(write_fd, &buffer[written], remaining);
		if ((size_t) res == remaining)
			return;
		if (res <= 0)
			throw NetworkException(concat("BinaryFDStream: write() failed: ", strerror(errno), "(", remaining, " requested, ", res, " written)"));

		if ((size_t) res > remaining)
			throw NetworkException(concat("BinaryFDStream: write() wrote too much bytes: ", remaining, " requested, ", res, " written"));
		written += res;
	}
	//fprintf(stderr, "BinaryFDStream: written %lu bytes\n", len);
}

void BinaryFDStream::write(BinaryWriteBuffer &buffer) {
	if (!buffer.isWriting())
		throw ArgumentException("cannot write() a BinaryWriteBuffer when not prepared for writing");
	while(!buffer.isFinished())
		writeNB(buffer);
}

void BinaryFDStream::writeNB(BinaryWriteBuffer &buffer) {
	if (!buffer.isWriting())
		throw ArgumentException("cannot writeNB() a BinaryWriteBuffer when not prepared for writing");

	auto written = writev(write_fd, (const iovec *) buffer.areas.data(), buffer.areas.size());
	if (written < 0)
		throw NetworkException(concat("BinaryFDStream: writev() failed: ", strerror(errno)));
	buffer.markBytesAsWritten(written);
	//printf("Wrote %d bytes of %d\n", (int) written, (int) buffer.size_total);
}



size_t BinaryFDStream::read(char *buffer, size_t len, bool allow_eof) {
	if (read_fd < 0)
		throw NetworkException(concat("BinaryFDStream: cannot read from closed socket ", read_fd, " in pid ", getpid()));
	if (is_eof)
		throw NetworkException("BinaryFDStream: tried to read from a socket which is eof'ed");

	size_t remaining = len;
	size_t bytes_read = 0;
	while (remaining > 0) {
		// Note: MSG_WAITALL does not guarantee that the whole buffer is filled; the loop is still required
		//auto r = ::recv(read_fd, buffer, remaining, MSG_WAITALL);
		auto r = ::read(read_fd, buffer, remaining);
		if (r == 0) {
			is_eof = true;
			if (!allow_eof || bytes_read > 0)
				throw NetworkException("BinaryFDStream: unexpected eof");
			return bytes_read;
		}
		if (r < 0)
			throw NetworkException(concat("BinaryFDStream: read() failed: ", strerror(errno)));
		bytes_read += r;
		buffer += r;
		remaining -= r;
	}
	if (bytes_read != len)
		throw NetworkException("BinaryFDStream: invalid read");

	//fprintf(stderr, "BinaryFDStream: read %lu bytes\n", bytes_read);
	return bytes_read;
}



/*
 * BinaryWriteBuffer
 */
BinaryWriteBuffer::BinaryWriteBuffer() : may_link(false), status(Status::CREATING), next_area_start(0), size_total(0), size_sent(0), areas_sent(0) {

}
BinaryWriteBuffer::~BinaryWriteBuffer() {
}

void BinaryWriteBuffer::write(const char *data, size_t len) {
	if (status != Status::CREATING)
		throw ArgumentException("cannot write() to a BinaryWriteBuffer after it was prepared for sending");

	// maybe we can just link to external memory, without touching our buffer
	if (may_link && len >= 64) {
		finishBufferedArea();
		areas.emplace_back(data, len);
		return;
	}

	// add it to our buffer. As the std::vector is allowed to reallocate the buffer, we need to adjust the areas if that happens.
	const char *vec_start = buffer.data();
	const char *vec_end = vec_start + buffer.size();

	buffer.reserve(buffer.size() + len);

	const char *new_vec_start = buffer.data();
	if (new_vec_start != vec_start) {
		for (int i=0;i<areas.size();i++) {
			if (areas[i].start >= vec_start && areas[i].start < vec_end)
				areas[i].start = areas[i].start - vec_start + new_vec_start;
		}
	}

	// the areas have been adjusted, now we can copy the data into our buffer
	buffer.insert(buffer.end(), data, data+len);

	// This must not happen (according to the C++ spec), but if it does, we want to know.
	if (buffer.data() != new_vec_start)
		throw ArgumentException("ERROR: BinaryWriteBuffer, buffer.insert() had a reallocation.");
}

size_t BinaryWriteBuffer::read(char *buffer, size_t len, bool allow_eof) {
	throw ArgumentException("A BinaryWriteBuffer cannot read()");
}

void BinaryWriteBuffer::finishBufferedArea() {
	if (next_area_start < buffer.size()) {
		const char *start_ptr = &(buffer[next_area_start]);
		size_t len = buffer.size() - next_area_start;
		areas.emplace_back(start_ptr, len);

		next_area_start = buffer.size();
	}
}

void BinaryWriteBuffer::prepareForWriting() {
	if (status != Status::CREATING)
		throw ArgumentException("cannot prepare a BinaryWriteBuffer for writing twice");

	finishBufferedArea();

	// count size
	size_total = 0;
	for (auto &area : areas)
		size_total += area.len;

	size_sent = 0;
	areas_sent = 0;

	status = Status::WRITING;
}

size_t BinaryWriteBuffer::getSize() {
	if (!isWriting())
		throw ArgumentException("BinaryWriteBuffer: cannot getSize() before prepareForWriting()");
	return size_total;
}

void BinaryWriteBuffer::markBytesAsWritten(size_t sent_bytes) {
	if (!isWriting())
		throw ArgumentException("cannot markBytesAsWritten() on a BinaryWriteBuffer when not writing");

	size_sent += sent_bytes;
	if (size_sent > size_total)
		throw ArgumentException("markBytesAsWritten() exceeds size of buffer");

	if (size_sent == size_total) {
		status = Status::FINISHED;
		return;
	}

	while (sent_bytes) {
		auto &area = areas.at(areas_sent);
		if (area.len <= sent_bytes) {
			areas_sent++;
			sent_bytes -= area.len;
		}
		else {
			area.start += sent_bytes;
			area.len -= sent_bytes;
			sent_bytes = 0;
		}
	}
}
