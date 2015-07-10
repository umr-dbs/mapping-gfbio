/*
 * blockingqueue.h
 *
 *  Created on: 17.05.2015
 *      Author: mika
 */

#ifndef BLOCKINGQUEUE_H_
#define BLOCKINGQUEUE_H_

#include "util/exceptions.h"

#include <mutex>
#include <condition_variable>
#include <deque>

template<typename T>
class BlockingQueue {
public:
	BlockingQueue();
	virtual ~BlockingQueue();
	T pop();
	void push(T &elem);
	void shutdown();
private:
	bool is_shutdown = false;
	std::deque<T> deque;
	std::mutex mutex;
	std::condition_variable cv;
};

template<typename T>
BlockingQueue<T>::BlockingQueue() {
}

template<typename T>
BlockingQueue<T>::~BlockingQueue() {
}

template<typename T>
void BlockingQueue<T>::push( T &value ) {
	{
		std::unique_lock<std::mutex> lock(mutex);
		deque.push_front(std::move(value));
	}
	this->cv.notify_one();
}

template<typename T>
T BlockingQueue<T>::pop() {
	std::unique_lock<std::mutex> lock(mutex);
	this->cv.wait(lock, [=]{ return !deque.empty() || is_shutdown; });
	if ( is_shutdown )
		throw ShutdownException("Queue closed");
	T rc(std::move(deque.back()));
	this->deque.pop_back();
	return rc;
}

template<typename T>
void BlockingQueue<T>::shutdown() {
	is_shutdown = true;
	cv.notify_all();
}

#endif /* BLOCKINGQUEUE_H_ */
