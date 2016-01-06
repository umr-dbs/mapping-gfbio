#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include <unistd.h>
#include <string>
#include <type_traits>
#include <vector>
#include <memory>


class BinaryWriteBuffer;

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
	public:
		virtual ~BinaryStream();
		virtual void write(const char *buffer, size_t len) = 0;
		virtual void write(BinaryWriteBuffer &buffer);

		void write(const std::string &string);
		void write(std::string &string) { write( (const std::string &) string); };
		template<typename T> void write(const T& t);
		template<typename T> void write(T& t);

		void flush();

		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) = 0;
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
		virtual ~BinaryFDStream();

		void close();

		using BinaryStream::write;
		using BinaryStream::read;
		virtual void write(const char *buffer, size_t len);
		virtual void write(BinaryWriteBuffer &buffer);
		void writeNB(BinaryWriteBuffer &buffer);
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false);

		int getReadFD() const { return read_fd; }
		int getWriteFD() const { return write_fd; }

		bool eof() { return is_eof; }
	private:
		bool is_eof;
		int read_fd;
		int write_fd;
};


/**
 * A buffer used for sending data as a batch.
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
		virtual void write(const char *buffer, size_t len) final;
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) final;

		void enableLinking() { may_link = true; }
		void disableLinking() { may_link = false; }

		bool isWriting() { return status == Status::WRITING; }
		bool isFinished() { return status == Status::FINISHED; }
		void prepareForWriting();
		size_t getSize();
		void markBytesAsWritten(size_t sent);
	private:
		void finishBufferedArea();

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

#endif
