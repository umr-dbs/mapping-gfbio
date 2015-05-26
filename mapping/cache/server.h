/*
 * server.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef SERVER_H_
#define SERVER_H_

#include "cache/blockingqueue.h"
#include "util/binarystream.h"
#include "util/log.h"
#include "raster/exceptions.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>
#include <mutex>

class Connection {
public:
	Connection(int fd) :
			fd(fd) {
		stream.reset(new UnixSocket(fd, fd));
	}
	~Connection() {
		Log::debug("Connection discarded. FD: %d", fd);
	}
	void process();
	const int fd;
private:
	std::unique_ptr<BinaryStream> stream;
};

class CacheServer {
	friend class std::thread;
public:
	static const uint8_t COMMAND_GET_RASTER = 1;

	static const uint8_t RESPONSE_OK = 1;
	static const uint8_t RESPONSE_PARTIAL = 2;
	static const uint8_t RESPONSE_ERROR = 9;

	CacheServer( int listenport, int num_threads ) : listenport(listenport), num_threads(num_threads) {}
	virtual ~CacheServer();
	void run();
	std::unique_ptr<std::thread> runAsync();
	virtual void stop();
protected:
	void main_loop();
	void thread_loop();
private:
	bool shutdown = false;
	int getListeningSocket(int port, int backlog = 10);
	BlockingQueue<std::unique_ptr<Connection>> queue;
	std::vector<std::unique_ptr<std::thread>> workers;
	std::vector<std::unique_ptr<Connection>> connections;
	int listenport;
	int num_threads;
	std::mutex connection_mtx;
};



#endif /* SERVER_H_ */
