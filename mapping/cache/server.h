/*
 * server.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef SERVER_H_
#define SERVER_H_

#include "cache/blockingqueue.h"
#include "cache/cachetask.h"
#include "cache/log.h"
#include "raster/exceptions.h"

#include <string>
#include <thread>
#include <memory>
#include <vector>

class Connection {
public:
	Connection(int fd) {
		stream.reset( new UnixSocket(fd,fd) );
	}
	void process();
private:
	std::unique_ptr<BinaryStream> stream;
};

class CacheServer {
	friend class std::thread;
public:
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
	int listenport;
	int num_threads;
};



#endif /* SERVER_H_ */
