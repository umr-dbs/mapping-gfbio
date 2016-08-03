#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include "util/make_unique.h"
#include "util/sha1.h"

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
 * Instead, we're opting for a very simple binary stream implementation.
 *
 * All IPC is buffered. While this incurs an additional copy, it is still a
 * performance improvement because it reduces syscalls to read() and write().
 *
 * Buffered IO is also required to implement nonblocking IO.
 *
 *
 * The default implementation of BinaryStream is based on posix file descriptors,
 * which are flexible enough for files, pipes, TCP etc. Unless there is a
 * use case for a stream that cannot be backed by a posix fd, there's no need
 * to turn BinaryStream into a virtual class hierarchy.
 */

class BinaryStream {
	public:
		/*
		 * The default constructor will create an unusable stream. Use the static constructors.
		 */
		BinaryStream();
		~BinaryStream();

		// BinaryStream is movable, but not copyable
		BinaryStream(const BinaryStream &) = delete;
		BinaryStream &operator=(const BinaryStream &) = delete;
		BinaryStream(BinaryStream &&);
		BinaryStream &operator=(BinaryStream &&);

		// Static constructors
		static BinaryStream connectURL(const std::string &url);
		static BinaryStream connectUNIX(const char *server_path);
		static BinaryStream connectTCP(const char *hostname, int port, bool no_delay = false);
		static BinaryStream fromAcceptedSocket(int socket, bool no_delay = false);
		static BinaryStream makePipe();

		/*
		 * A stream can be either blocking or nonblocking.
		 *
		 * A blocking stream is guaranteed to read() or write() the exact amount of bytes requested.
		 * The downside is that some streams can only guarantee this by blocking program execution for a while,
		 * e.g. while waiting for new data to arrive over the network.
		 *
		 * Non-blocking means that partial reads and writes are possible. Always check the buffer's state
		 * to know whether the buffer was fully read or written.
		 *
		 * These methods switch between the two modes.
		 */
		void makeNonBlocking();
		/*
		 * @see makeNonBlocking()
		 */
		void makeBlocking();

		/*
		 * Write the contents of a BinaryWriteBuffer to the stream (blocking)
		 */
		void write(BinaryWriteBuffer &buffer);
		/*
		 * Write the contents of a BinaryWriteBuffer to the stream (non-blocking)
		 */
		void writeNB(BinaryWriteBuffer &buffer);
		/*
		 * Fill a BinaryReadBuffer with contents from the stream (blocking)
		 * @return true if eof was encountered and allow_eof = true, otherwise false
		 */
		bool read(BinaryReadBuffer &buffer, bool allow_eof = false);
		/*
		 * Fill a BinaryReadBuffer with contents from the stream (non-blocking)
		 * @return true if eof was encountered and allow_eof = true, otherwise false
		 */
		bool readNB(BinaryReadBuffer &buffer, bool allow_eof = false);

		/*
		 * Returns the file descriptor used for reading.
		 * Don't manipulate the FD in any way; this shall only be used for select() etc
		 */
		int getReadFD() const { return read_fd; }
		/*
		 * Returns the file descriptor used for writing.
		 * Don't manipulate the FD in any way; this shall only be used for select() etc
		 */
		int getWriteFD() const { return write_fd; }

		/*
		 * Closes all file descriptors
		 */
		void close();
	private:
		// This constructor is private. Use the static named constructors instead.
		BinaryStream(int read_fd, int write_fd);

		bool is_blocking;
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

		/*
		 * Get a SHA1 hash of this buffer's contents
		 */
		SHA1::SHA1Value hash();

		bool isWriting() { return status == Status::WRITING; }
		bool isFinished() { return status == Status::FINISHED; }
		void markBytesAsWritten(size_t written);
	private:
		void finishBufferedArea();
		void prepareForWriting();

		std::vector<char> buffer;
		std::vector<Area> areas;
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
	/*
	 * First, add some helper templates to modify and classify types.
	 */

	// strip references, const and volatile from the type
	template <typename T>
	using remove_rcv_t = typename std::remove_cv< typename std::remove_reference<T>::type >::type;

	// is a primitive type: ints, floats and enums
	template <typename T>
	using is_primitive = std::integral_constant<bool,
			std::is_arithmetic<remove_rcv_t<T>>::value
			|| std::is_enum<remove_rcv_t<T>>::value
		>;

	// is a vector
	template <typename T> struct is_vector : std::false_type {};
	template <typename T> struct is_vector< std::vector<T> > : std::true_type {};

	// is a class with a .serialize() method
	// TODO: this is a broken heuristic for now, meaning any class that's not a std::string or std::vector
	template <typename T>
	using is_serializable = std::integral_constant<bool,
			std::is_class<remove_rcv_t<T>>::value
			&& !std::is_same< remove_rcv_t<T>, std::string>::value
			&& !is_vector< remove_rcv_t<T> >::value
		>;

	/*
	 * Now define the buffer_write_helper() method, which writes the given argument to the buffer.
	 */
	// For primitive types
	template <typename T>
	typename std::enable_if< is_primitive<T>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& t, bool is_persistent_memory) {
		buffer.write((const char *) &t, sizeof(T), is_persistent_memory);
	}

	// For serializable classes
	template <typename T>
	typename std::enable_if< is_serializable<T>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& t, bool is_persistent_memory) {
		t.serialize(buffer, is_persistent_memory);
	}

	// std::string
	template <typename T>
	typename std::enable_if< std::is_same< remove_rcv_t<T>, std::string>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, T&& str, bool is_persistent_memory) {
		buffer.writeString(str, is_persistent_memory);
	}

	// std::vector of a primitive type
	// These are guaranteed to be continuous in memory, so we can just write the whole array at once
	template <typename T>
	typename std::enable_if< is_primitive<T>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, const std::vector<T> &vec, bool is_persistent_memory) {
		buffer.write(vec.size());
		buffer.write((char *) vec.data(), vec.size()*sizeof(T), is_persistent_memory);
	}

	// std::vector of a serializable class or std::string
	// We must serialize each element sequentially
	template <typename T>
	typename std::enable_if< is_serializable<T>::value || std::is_same< remove_rcv_t<T>, std::string>::value >::type
		buffer_write_helper(BinaryWriteBuffer &buffer, const std::vector<T> &vec, bool is_persistent_memory) {
		buffer.write(vec.size());
		for (size_t i=0;i<vec.size();i++)
			buffer.write(vec[i], is_persistent_memory);
	}
}


template<typename T> void BinaryWriteBuffer::write(T&& t, bool is_persistent_memory) {
	detail::buffer_write_helper(*this, std::forward<T>(t), is_persistent_memory);
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
		void read(char *buffer, size_t len);
		void read(std::string *string);
		template<typename T> typename std::enable_if< detail::is_primitive<T>::value>::type
			read(T *t) { read((char *) t, sizeof(T)); }

		// easier deserialization of std::vector
		// std::vector of a primitive type
		template<typename T> typename std::enable_if< detail::is_primitive<T>::value>::type
			read(std::vector<T> *vec) {
				auto size = read<size_t>();
				vec->resize(size);
				read((char *) vec->data(), size*sizeof(T));
			}
		// std::vector of a serializable class
		template<typename T> typename std::enable_if< detail::is_serializable<T>::value>::type
			read(std::vector<T> *vec) {
				auto size = read<size_t>();
				vec->clear();
				vec->reserve(size);
				for (size_t i=0;i<size;i++)
					vec->emplace_back(*this);
			}
		// std::vector of a std::string
			void read(std::vector<std::string> *vec) {
				auto size = read<size_t>();
				vec->resize(size);
				for (size_t i=0;i<size;i++)
					read(&((*vec)[i]));
			}


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

		// This is required by the unit tests to compare to buffers for equality. Don't use it anywhere else.
		friend void compareBinaryReadBuffers(const BinaryReadBuffer &a, const BinaryReadBuffer &b);
};


#endif
