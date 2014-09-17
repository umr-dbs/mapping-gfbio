#ifndef UTIL_SOCKET_H
#define UTIL_SOCKET_H

#include <unistd.h>
#include <string>
#include <type_traits>

class Socket {
	public:
		Socket(const char *server_path);
		Socket(int read_fd, int write_fd = -2);
		~Socket();
		Socket(const Socket &) = delete;
		Socket &operator=(const Socket &) = delete;

		void close();

		void write(const char *buffer, size_t len);
		void write(const std::string &string);
		void write(std::string &string) { write( (const std::string &) string); };
		template<typename T> void write(const T& t);
		template<typename T> void write(T& t);

		size_t read(char *buffer, size_t len, bool allow_eof = false);
		size_t read(std::string *string, bool allow_eof = false);
		// Note: reading classes must be implemented via constructors, e.g. QueryRectangle rect(socket)
		template<typename T> typename std::enable_if< !std::is_class<T>::value, size_t>::type
			read(T *t, bool allow_eof = false) { return read((char *) t, sizeof(T), allow_eof); }

		bool eof() { return is_eof; }
	private:
		bool is_eof;
		int read_fd;
		int write_fd;
};


// We need to make sure that classes are never serialized by their binary representation; always call toSocket() on them
template <typename T>
typename std::enable_if< !std::is_class<T>::value >::type socket_write_helper(Socket &socket, T& t) {
	socket.write((const char *) &t, sizeof(T));
}

template <typename T>
typename std::enable_if< std::is_class<T>::value >::type socket_write_helper(Socket &socket, T& t) {
	t.toSocket(socket);
}

template<typename T> void Socket::write(const T& t) {
	socket_write_helper<const T>(*this, t);
}

template<typename T> void Socket::write(T& t) {
	socket_write_helper<T>(*this, t);
}




#endif
