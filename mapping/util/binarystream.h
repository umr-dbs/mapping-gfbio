#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include <unistd.h>
#include <string>
#include <type_traits>

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

		void write(const std::string &string);
		void write(std::string &string) { write( (const std::string &) string); };
		template<typename T> void write(const T& t);
		template<typename T> void write(T& t);

		virtual size_t read(char *buffer, size_t len, bool allow_eof = false) = 0;
		size_t read(std::string *string, bool allow_eof = false);
		/*
		 * Note: reading classes must be implemented via constructors or static getters, e.g.
		 * QueryRectangle rect(stream)
		 * auto raster = GenericRaster::fromStream(stream)
		 */
		template<typename T> typename std::enable_if< !std::is_class<T>::value, size_t>::type
			read(T *t, bool allow_eof = false) { return read((char *) t, sizeof(T), allow_eof); }
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
 * An implementation using Unix Sockets
 *
 * Unix sockets are a lot faster than TCP sockets, since they don't need to wrap the data in TCP packets with headers.
 *
 *
 */
class UnixSocket : public BinaryStream {
	public:
		UnixSocket(const char *server_path);
		UnixSocket(const char *hostname, int port, bool no_delay = false);
		UnixSocket(int read_fd, int write_fd = -2, bool no_delay = false);
		virtual ~UnixSocket();

		void close();

		virtual void write(const char *buffer, size_t len);
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false);

		int getReadFD() const { return read_fd; }
		int getWriteFD() const { return write_fd; }

		bool eof() { return is_eof; }
	private:
		bool is_eof;
		int read_fd;
		int write_fd;
};


class CountingStream : public BinaryStream {
	public:
		CountingStream();
		virtual ~CountingStream();

		virtual void write(const char *buffer, size_t len);
		virtual size_t read(char *buffer, size_t len, bool allow_eof = false);

		size_t getBytesRead() { return bytes_read; }
		size_t getBytesWritten() { return bytes_written; }
	private:
		size_t bytes_read;
		size_t bytes_written;
};


#endif
