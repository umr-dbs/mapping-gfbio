#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include <unistd.h>
#include <string>
#include <type_traits>
#include <vector>
#include <memory>

#include "util/make_unique.h"

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
 * Instead, we're opting for a very simple binary stream implementation.
 *
 * All IPC is buffered. While this incurs an additional copy, it is still a
 * performance improvement because it reduces syscalls to read() and write().
 *
 * Buffered IO is also required to implement nonblocking IO.
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
		 * Non-blocking means that partial reads and writes are possible. Always check the buffer's state
		 * to know whether a read or write was completed.
		 *
		 * This method turns a stream into nonblocking mode. There is currently no way to turn them back;
		 * this may be implemented when a need arises.
		 */
		virtual void makeNonBlocking();

		/*
		 * Write the contents of a BinaryWriteBuffer to the stream (blocking)
		 */
		virtual void write(BinaryWriteBuffer &buffer);
		/*
		 * Fill a BinaryReadBuffer with contents from the stream (blocking)
		 */
		virtual void read(BinaryReadBuffer &buffer);

		void flush() {}; // TODO: remove
};


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

		virtual void write(BinaryWriteBuffer &buffer);
		void writeNB(BinaryWriteBuffer &buffer);
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
class BinaryWriteBuffer {
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
		~BinaryWriteBuffer();

		/*
		 * Methods for writing.
		 */

		/*
		 * Write a couple of raw bytes from a buffer
		 *
		 * @param buffer pointer to the first byte
		 * @param len number of bytes to write
		 * @param is_persistent_memory signals that the buffer will remain available after
		 *        the write() call. This information can be used to avoid copies.
		 */
		void write(const char *buffer, size_t len, bool is_persistent_memory = false);

		/*
		 * This will serialize based on the type. See stream_write_helper below.
		 * - Native types will be serialized by their binary represenation.
		 * - std::string has its own serialization function.
		 * - On all other classes, this will call object.serialize(buffer).
		 */
		template<typename T> void write(T&& t, bool is_persistent_memory = false);
		/*
		 * operator<< overload for code brevity.
		 */
		template<typename T>
		BinaryWriteBuffer &operator<<(T&& t) {
			write(std::forward<T>(t), false);
			return *this;
		}

		/*
		 * We cannot add a serialize() method to std:: classes, so we use these helper methods.
		 * Don't call them directly; use buffer.write(obj)
		 */
		void writeString(const std::string &str, bool is_persistent_memory = false);

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


/*
 * buffer_write_helper is writing a type into a buffer.
 */
namespace detail {
	// strip references, const and volatile from the type
	template <typename T>
	using remove_rcv_t = typename std::remove_cv< typename std::remove_reference<T>::type >::type;

	// ints, floats and enums
	template <typename T>
	typename std::enable_if< std::is_arithmetic<remove_rcv_t<T>>::value || std::is_enum<remove_rcv_t<T>>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& t) {
		buffer.write((const char *) &t, sizeof(T), false);
	}

	// classes except std::string
	template <typename T>
	typename std::enable_if< std::is_class<remove_rcv_t<T>>::value && !std::is_same< remove_rcv_t<T>, std::string>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& t) {
		t.serialize(buffer);
	}

	// std::string
	template <typename T>
	typename std::enable_if< std::is_same< remove_rcv_t<T>, std::string>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& str) {
		buffer.writeString(str);
	}
}


template<typename T> void BinaryWriteBuffer::write(T&& t, bool is_persistent_memory) {
	detail::buffer_write_helper<T>(*this, std::forward<T>(t));
}


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
 * Sometimes, a writebuffer needs to manage the lifetime of another object, mostly when part of the object's memory
 * was linked in the buffer. This is an attempt at a helper class.
 */
template<typename T>
class BinaryWriteBufferWithSharedObject : public BinaryWriteBuffer {
	public:
		BinaryWriteBufferWithSharedObject( std::shared_ptr<T> obj ) : object(obj) {}
	private:
		std::shared_ptr<T> object;
};



/**
 * A buffer used for reading a batch of data sent using a BinaryWriteBuffer
 *
 * This is required for nonblocking reads: the buffer can be filled in a nonblocking manner, when it's
 * full it can be deserialized and processed.
 */
class BinaryReadBuffer {
		friend class BinaryStream;
		friend class BinaryFDStream;
		enum class Status {
			READING_SIZE,
			READING_DATA,
			FINISHED
		};
	public:
		BinaryReadBuffer();
		~BinaryReadBuffer();

		/*
		 * Methods for reading.
		 *
		 * Note: reading classes must be implemented via constructors or static getters, e.g.
		 * QueryRectangle rect(buffer)
		 * auto raster = GenericRaster::deserialize(buffer)
		 */
		size_t read(char *buffer, size_t len, bool allow_eof = false);
		size_t read(std::string *string, bool allow_eof = false);
		template<typename T> typename std::enable_if< !std::is_class<T>::value, size_t>::type
			read(T *t, bool allow_eof = false) { return read((char *) t, sizeof(T), allow_eof); }
		template<typename T>
			T read() {
				T t; read(&t); return t;
		}

		bool isRead() const { return status == Status::FINISHED; }
		bool isEmpty() const { return size_read == 0 && status == BinaryReadBuffer::Status::READING_SIZE; }
		size_t getPayloadSize() const;
		void markBytesAsRead(size_t read);
	private:
		void prepareBuffer(size_t expected_size);
		std::vector<char> buffer;
		Status status;
		size_t size_total, size_read;
};


#endif
