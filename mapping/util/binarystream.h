#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include <unistd.h>
#include <string>
#include <type_traits>
#include <vector>
#include <memory>


class BinaryWriteBuffer;
class BinaryReadBuffer;

/*
 * This is a stream class for IPC, meant to allow serialization of objects.
 *
 * We are not using std::iostream for several reasons.
 * First, the default overloads are meant for human-readable display, not
 * for efficient binary serialization.
 * Second, they're not reversible:
 *   out << a << b << c
 * will result in a stream that cannot be correctly deserialized using
 *   in >> a >> b >> c
 * Third, deserialization using operator>> will only work for mutable objects
 * with a default constructor. Rasters do not fit that definition.
 *
 * Instead, we're opting for a very simple binary stream implementation
 * providing nothing but read() and write() functions, including some
 * overloads.
 */

class BinaryStream {
	protected:
		BinaryStream();
		BinaryStream(const BinaryStream &) = delete;
		BinaryStream &operator=(const BinaryStream &) = delete;

		bool is_blocking;
	public:
		virtual ~BinaryStream();

		/*
		 * A stream can be either blocking or nonblocking.
		 *
		 * A blocking stream is guaranteed to read() or write() the exact amount of bytes requested.
		 * The downside is that some streams can only guarantee this by blocking program execution for a while,
		 * e.g. while waiting for new data to arrive over the network.
		 *
		 * Non-blocking means that partial reads and writes are possible. This is only useful when
		 * combined with buffering.
		 * To make sure that no attempts are made to do unbuffered reads or writes on a nonblocking stream,
		 * the default read() and write() methods are disabled on nonblocking streams.
		 *
		 * This method turns a stream into nonblocking mode. There is currently no way to turn them back;
		 * this may be implemented when a need arises.
		 */
		virtual void makeNonBlocking();

		/*
		 * Write a couple of raw bytes from a buffer
		 *
		 * @param buffer pointer to the first byte
		 * @param len number of bytes to write
		 * @param is_persistent_memory signals that the buffer will remain available after
		 *        the write() call. This information can be used to avoid copies.
		 */
		virtual void write(const char *buffer, size_t len, bool is_persistent_memory = false) = 0;
		virtual void write(BinaryWriteBuffer &buffer);

		void write(const std::string &string, bool is_persistent_memory = false);
		void write(std::string &string) { write( (const std::string &) string); };
		template<typename T> void write(const T& t);
		template<typename T> void write(T& t);

		void flush();

		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) = 0;
		virtual void read(BinaryReadBuffer &buffer);
		size_t read(std::string *string, bool allow_eof = false);
		/*
		 * Note: reading classes must be implemented via constructors or static getters, e.g.
		 * QueryRectangle rect(stream)
		 * auto raster = GenericRaster::fromStream(stream)
		 */
		template<typename T> typename std::enable_if< !std::is_class<T>::value, size_t>::type
			read(T *t, bool allow_eof = false) { return read((char *) t, sizeof(T), allow_eof); }
		template<typename T>
			T read() {
				T t; read(&t); return t;
		}
};


// We need to make sure that classes are never serialized by their binary representation; always call toStream() on them
template <typename T>
typename std::enable_if< !std::is_class<T>::value >::type stream_write_helper(BinaryStream &stream, T& t) {
	stream.write((const char *) &t, sizeof(T));
}

template <typename T>
typename std::enable_if< std::is_class<T>::value >::type stream_write_helper(BinaryStream &stream, T& t) {
	t.toStream(stream);
}

template<typename T> void BinaryStream::write(const T& t) {
	stream_write_helper<const T>(*this, t);
}

template<typename T> void BinaryStream::write(T& t) {
	stream_write_helper<T>(*this, t);
}


/**
 * An implementation using posix file descriptors
 *
 * Currently used for AF_UNIX and TCP socket connections.
 */
class BinaryFDStream : public BinaryStream {
	public:
		BinaryFDStream(const char *server_path);
		BinaryFDStream(const char *hostname, int port, bool no_delay = false);
		BinaryFDStream(int read_fd, int write_fd = -2, bool no_delay = false);
		struct PIPE_t {};
		static constexpr PIPE_t PIPE = {};
		BinaryFDStream(PIPE_t p);

		virtual ~BinaryFDStream();

		virtual void makeNonBlocking();

		void close();

		using BinaryStream::write;
		using BinaryStream::read;
		virtual void write(const char *buffer, size_t len, bool is_persistent_memory = false);
		virtual void write(BinaryWriteBuffer &buffer);
		void writeNB(BinaryWriteBuffer &buffer);
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false);
		virtual void read(BinaryReadBuffer &buffer);
		bool readNB(BinaryReadBuffer &buffer, bool allow_eof = false);

		int getReadFD() const { return read_fd; }
		int getWriteFD() const { return write_fd; }

		bool eof() { return is_eof; }
	private:
		bool is_eof;
		int read_fd;
		int write_fd;
};


/**
 * A buffer used for sending data as a batch, always prefixed with the packet length.
 *
 * This is required when sending data over TCP sockets with NODELAY.
 * Otherwise, every single write() call would generate a new TCP packet.
 *
 * Because we need to support large data (e.g. rasters) without copying all its memory into a buffer,
 * this buffer supports linking external memory. The programmer must take care to guarantee that the
 * external memory is neither changed nor deallocated before this buffer is sent.
 */
class BinaryWriteBuffer : public BinaryStream {
		friend class BinaryStream;
		friend class BinaryFDStream;
		struct Area {
			Area(const char *start, size_t len) : start(start), len(len) {}
			const char *start;
			size_t len;
		};
		enum class Status {
			CREATING,
			WRITING,
			FINISHED
		};
	public:
		BinaryWriteBuffer();
		virtual ~BinaryWriteBuffer();

		using BinaryStream::write;
		using BinaryStream::read;
		virtual void write(const char *buffer, size_t len, bool is_persistent_memory = false) final;
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) final;

		void enableLinking() { may_link = true; }
		void disableLinking() { may_link = false; }

		bool isWriting() { return status == Status::WRITING; }
		bool isFinished() { return status == Status::FINISHED; }
		size_t getSize();
		void markBytesAsWritten(size_t written);
	private:
		void finishBufferedArea();
		void prepareForWriting();

		std::vector<char> buffer;
		std::vector<Area> areas;
		bool may_link;
		Status status;

		size_t next_area_start;

		size_t size_total;
		size_t size_sent;
		size_t areas_sent;
};


/**
 * Sometimes, a writebuffer needs to manage the lifetime of another object, mostly when part of the object's memory
 * was linked in the buffer. This is an attempt at a helper class.
 */
template<typename T>
class BinaryWriteBufferWithObject : public BinaryWriteBuffer {
	public:
		std::unique_ptr<T> object;
};


/**
 * A buffer used for reading a batch of data sent using a BinaryWriteBuffer
 *
 * This is required for nonblocking reads: the buffer can be filled in a nonblocking manner, when it's
 * full it can be deserialized and processed.
 */
class BinaryReadBuffer : public BinaryStream {
		friend class BinaryStream;
		friend class BinaryFDStream;
		enum class Status {
			READING_SIZE,
			READING_DATA,
			FINISHED
		};
	public:
		BinaryReadBuffer();
		virtual ~BinaryReadBuffer();

		using BinaryStream::write;
		using BinaryStream::read;
		virtual void write(const char *buffer, size_t len, bool is_persistent_memory = false) final;
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) final;

		bool isRead() { return status == Status::FINISHED; }
		bool isEmpty() { return size_read == 0 && status == BinaryReadBuffer::Status::READING_SIZE; }
		size_t getPayloadSize();
		void markBytesAsRead(size_t read);
	private:
		void prepareBuffer(size_t expected_size);
		std::vector<char> buffer;
		Status status;
		size_t size_total, size_read;
};


#endif
