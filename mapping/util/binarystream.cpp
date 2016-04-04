#include "util/binarystream.h"
#include "util/exceptions.h"

#include <string.h> // memset(), strerror()
#include <errno.h>
#include <memory>
#include <algorithm>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <fcntl.h>


/*
 * BinaryStream
 * Construction, Move and Cleanup
 */
BinaryStream::BinaryStream() : is_blocking(true), read_fd(-1), write_fd(-1) {
}
BinaryStream::BinaryStream(int read_fd, int write_fd) : is_blocking(true), read_fd(read_fd), write_fd(write_fd) {
}

BinaryStream::~BinaryStream() {
	close();
}

void BinaryStream::close() {
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

BinaryStream::BinaryStream(BinaryStream &&other) : is_blocking(true), read_fd(-1), write_fd(-1) {
	*this = std::move(other);
}

BinaryStream &BinaryStream::operator=(BinaryStream &&other) {
	std::swap(is_blocking, other.is_blocking);
	std::swap(read_fd, other.read_fd);
	std::swap(write_fd, other.write_fd);
	return *this;
}


/*
 * BinaryStream
 * Static constructors
 */
BinaryStream BinaryStream::connectUNIX(const char *server_path) {
	int new_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (new_fd < 0)
		throw NetworkException("BinaryStream: unable to create socket()");

	struct sockaddr_un server_addr;
	memset((void *) &server_addr, 0, sizeof(server_addr));

	server_addr.sun_family = AF_UNIX;
	strcpy(server_addr.sun_path, server_path);
	if (connect(new_fd, (sockaddr *) &server_addr, sizeof(server_addr)) == -1) {
		::close(new_fd);
		throw NetworkException("BinaryStream: unable to connect()");
	}

	return BinaryStream(new_fd, new_fd);
}

BinaryStream BinaryStream::connectTCP(const char *hostname, int port, bool no_delay) {
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
		throw NetworkException("BinaryStream: unable to create socket()");
	}

	if (connect(new_fd, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
		freeaddrinfo(servinfo);
		::close(new_fd);
		throw NetworkException(concat("BinaryStream: unable to connect(", hostname, ":", port, "/", portstr, "): ", strerror(errno)));
	}
	freeaddrinfo(servinfo);

	if (no_delay) {
		int one = 1;
		setsockopt(new_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
	}

	return BinaryStream(new_fd, new_fd);
}

BinaryStream BinaryStream::fromAcceptedSocket(int socket, bool no_delay) {
	if (no_delay) {
		int one = 1;
		setsockopt(socket, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
	}

	return BinaryStream(socket, socket);
}

BinaryStream BinaryStream::makePipe() {
	int fds[2];
	auto res = pipe(fds);
	if (res != 0)
		throw NetworkException(concat("pipe() call failed: ", strerror(errno)));

	return BinaryStream(fds[0], fds[1]);
}


/*
 * BinaryStream
 * Make nonblocking
 */
static void set_fd_nonblocking(int fd, bool nonblock) {
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1)
		throw NetworkException("Cannot change blocking mode on fd, fcntl failed");
	int new_flags = flags;
	if (nonblock)
		new_flags |= O_NONBLOCK;
	else
		new_flags &= ~O_NONBLOCK;
	int res = fcntl(fd, F_SETFL, new_flags);
	if (res == -1)
		throw NetworkException("Cannot change blocking mode on fd, fcntl failed");
}

void BinaryStream::makeNonBlocking() {
	if (!is_blocking)
		throw ArgumentException("BinaryStream::makeNonBlocking(): is already nonblocking");

	set_fd_nonblocking(read_fd, true);
	set_fd_nonblocking(write_fd, true);
	is_blocking = false;
}

void BinaryStream::makeBlocking() {
	if (is_blocking)
		throw ArgumentException("BinaryStream::makeBlocking(): is already blocking");

	set_fd_nonblocking(read_fd, false);
	set_fd_nonblocking(write_fd, false);
	is_blocking = true;
}



/*
 * BinaryStream
 * Reading and Writing
 */
void BinaryStream::write(BinaryWriteBuffer &buffer) {
	if (!is_blocking)
		throw NetworkException("Cannot write() to a nonblocking stream");

	buffer.prepareForWriting();
	if (!buffer.isWriting())
		throw ArgumentException("cannot write() a BinaryWriteBuffer when not prepared for writing");

	while (!buffer.isFinished())
		writeNB(buffer);
}

void BinaryStream::writeNB(BinaryWriteBuffer &buffer) {
	buffer.prepareForWriting();
	if (!buffer.isWriting())
		throw ArgumentException("cannot writeNB() a BinaryWriteBuffer when not prepared for writing");

	auto written = writev(write_fd, (const iovec *) &buffer.areas.at(buffer.areas_sent), buffer.areas.size()-buffer.areas_sent);
	if (written < 0) {
		if (!is_blocking && (errno == EAGAIN || errno == EWOULDBLOCK))
			return;
		throw NetworkException(concat("BinaryStream: writev() failed: ", strerror(errno)));
	}
	if (written == 0) {
		if (is_blocking)
			throw NetworkException(concat("BinaryStream: writev() wrote 0 bytes in blocking call"));
		return;
	}
	buffer.markBytesAsWritten(written);
	//printf("Wrote %d bytes of %d\n", (int) written, (int) buffer.size_total);
}


bool BinaryStream::read(BinaryReadBuffer &buffer, bool allow_eof) {
	if (!is_blocking)
		throw NetworkException("Cannot read() on a nonblocking stream");

	while (!buffer.isRead()) {
		if (readNB(buffer, allow_eof) == true)
			return true;
	}
	return false;
}

bool BinaryStream::readNB(BinaryReadBuffer &buffer, bool allow_eof) {
	if (buffer.isRead())
		throw ArgumentException("cannot read() a BinaryReadBuffer that's already fully read");

	auto left_to_read = buffer.size_total-buffer.size_read;

	auto bytes_read = ::read(read_fd, buffer.buffer.data()+buffer.size_read, buffer.size_total-buffer.size_read);
	if (bytes_read == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return false;
		throw NetworkException(concat("BinaryStream: unexpected error while reading a BinaryReadBuffer: ", strerror(errno)));
	}
	if (bytes_read == 0) {
		if (!allow_eof || !buffer.isEmpty())
			throw NetworkException("BinaryStream: unexpected eof while reading a BinaryReadBuffer");
		return true;
	}
	buffer.markBytesAsRead(bytes_read);
	return false;
}


/*
 * BinaryWriteBuffer
 */
BinaryWriteBuffer::BinaryWriteBuffer() : status(Status::CREATING), next_area_start(0), size_total(0), size_sent(0), areas_sent(0) {
	// always prefix with the size
	areas.emplace_back((const char *) &size_total, sizeof(size_total));
}
BinaryWriteBuffer::~BinaryWriteBuffer() {
}

void BinaryWriteBuffer::write(const char *data, size_t len, bool is_persistent_memory) {
	if (status != Status::CREATING)
		throw ArgumentException("cannot write() to a BinaryWriteBuffer after it was prepared for sending");

	// maybe we can just link to external memory, without touching our buffer
	if (is_persistent_memory && len >= 64) {
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
		throw MustNotHappenException("ERROR: BinaryWriteBuffer, buffer.insert() had a reallocation.");
}

void BinaryWriteBuffer::writeString(const std::string &string, bool is_persistent_memory) {
	size_t len = string.size();
	if (len > (size_t) (1<<31))
		throw NetworkException("BinaryStream: String too large to transmit");
	write(len);
	write(string.data(), len, is_persistent_memory);
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
	if (status == Status::CREATING) {
		finishBufferedArea();

		// count size
		size_total = 0;
		for (auto &area : areas)
			size_total += area.len;

		size_sent = 0;
		areas_sent = 0;

		status = Status::WRITING;
	}
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
		throw ArgumentException(concat("markBytesAsWritten() exceeds size of buffer, ", sent_bytes, " sent, now at ", size_sent, " of ", size_total));

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


/*
 * BinaryReadBuffer
 */
BinaryReadBuffer::BinaryReadBuffer() {
	status = Status::READING_SIZE;
	prepareBuffer(sizeof(size_t));
}
BinaryReadBuffer::~BinaryReadBuffer() {

}

void BinaryReadBuffer::read(char *buffer, size_t len) {
	if (status != Status::FINISHED)
		throw ArgumentException("cannot read() from a BinaryReadBuffer until it has been filled");

	size_t remaining = size_total - size_read;
	if (remaining < len)
		throw NetworkException(concat("BinaryReadBuffer: not enough data to satisfy read, ", remaining, " of ", size_total, " remaining, ", len, " requested"));

	// copy data where it should go.
	const char *vec_start = this->buffer.data() + size_read;
	memcpy(buffer, vec_start, len);
	size_read += len;
}

void BinaryReadBuffer::read(std::string *string) {
	auto len = read<size_t>();

	std::unique_ptr<char[]> buffer( new char[len] );
	read(buffer.get(), len);
	string->assign(buffer.get(), len);
}


size_t BinaryReadBuffer::getPayloadSize() const {
	if (status != Status::FINISHED)
		throw ArgumentException("cannot getPayloadSize() from a BinaryReadBuffer until it has been filled");
	return size_total;
}

void BinaryReadBuffer::prepareBuffer(size_t expected_size) {
	size_read = 0;
	size_total = expected_size;
	buffer.assign(size_total, 0); // TODO: can we avoid pre-filling this vector?
}

void BinaryReadBuffer::markBytesAsRead(size_t read) {
	size_read += read;
	if (size_read > size_total)
		throw MustNotHappenException(concat("Internal logic error: BinaryReadBuffer, size_read = ", size_read, " > size_total = ", size_total));

	if (size_read == size_total) {
		if (status == Status::READING_SIZE) {
			status = Status::READING_DATA;
			auto expected_size = *((size_t *) buffer.data()) - sizeof(size_t);
			prepareBuffer(expected_size);
		}
		else if (status == Status::READING_DATA) {
			status = Status::FINISHED;
			size_read = 0;
		}
		else
			throw MustNotHappenException("Internal logic error: BinaryReadBuffer was read in an invalid state");
	}
}
