/*
 * common.h
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#ifndef CACHE_COMMON_H_
#define CACHE_COMMON_H_ 1

#include "util/binarystream.h"
#include "util/exceptions.h"
#include "util/log.h"
#include <gdal_priv.h>
#include <memory>

#include <sstream>
#include <cstring>
#include <sys/select.h>
#include <errno.h>
#include <chrono>
#include <iostream>
#include <mutex>

//#define ENABLE_TIMING

#ifndef ENABLE_TIMING
#define TIME_EXEC(name) ((void)0)
#define TIME_EXEC2(name) ((void)0)
#endif

#ifdef ENABLE_TIMING
#define TIME_EXEC(name) ExecTimer t(name)
#define TIME_EXEC2(name) ExecTimer t2(name)
#endif


//
// Provides helper functions for common tasks.
//

class CacheCube;
class QueryRectangle;
class SpatioTemporalReference;
class GridSpatioTemporalResult;

/**
 * Helper class to track execution time
 */
class ExecTimer {
public:
	ExecTimer( std::string &&name );
	ExecTimer() = delete;
	ExecTimer( const ExecTimer& ) = delete;
	ExecTimer( ExecTimer&& ) = delete;
	ExecTimer& operator=(const ExecTimer& ) = delete;
	ExecTimer& operator=( ExecTimer&& ) = delete;
	~ExecTimer();
private:
	static thread_local uint8_t depth;
	static thread_local std::ostringstream buffer;
	std::string name;
	std::chrono::time_point<std::chrono::system_clock> start;
};


/**
 * Simple multi-reader, single writer lock
 */
class RWLock {
public:
	RWLock() : read_count(0) {};

	void lock_shared() {
		std::lock_guard<std::mutex> g(w_prio);
		std::lock_guard<std::mutex> g2(mtx);
		read_count++;
		if ( read_count == 1 )
			w_lock.lock();
	};

	void unlock_shared() {
		std::lock_guard<std::mutex> g(mtx);
		read_count--;
		if ( read_count == 0 )
			w_lock.unlock();
	}

	void lock_exclusive() {
		std::lock_guard<std::mutex> g(w_prio);
		w_lock.lock();
	};

	void unlock_exclusive() {
		w_lock.unlock();
	};
private:
	int read_count;
	std::mutex w_prio;
	std::mutex mtx;
	std::mutex w_lock;
};

/**
 * Guard for obtaining a read-lock
 */
class SharedLockGuard {
public:
	SharedLockGuard() = delete;
	SharedLockGuard( SharedLockGuard &&) = delete;
	SharedLockGuard( const SharedLockGuard &) = delete;
	SharedLockGuard& operator=(const SharedLockGuard &) = delete;
	SharedLockGuard& operator=(SharedLockGuard &&) = delete;
	SharedLockGuard( RWLock &lock ) : lock(lock) {
		lock.lock_shared();
	}
	~SharedLockGuard() {
		lock.unlock_shared();
	}
private:
	RWLock &lock;
};

/**
 * Guard for obtaining a write-lock
 */
class ExclusiveLockGuard {
public:
	ExclusiveLockGuard() = delete;
	ExclusiveLockGuard( ExclusiveLockGuard &&) = delete;
	ExclusiveLockGuard( const ExclusiveLockGuard &) = delete;
	ExclusiveLockGuard& operator=(const ExclusiveLockGuard &) = delete;
	ExclusiveLockGuard& operator=(ExclusiveLockGuard &&) = delete;
	ExclusiveLockGuard( RWLock &lock ) : lock(lock) {
		lock.lock_exclusive();
	}
	~ExclusiveLockGuard() {
		lock.unlock_exclusive();
	}
private:
	RWLock &lock;
};


/**
 * Class holding utility function used by various cache components
 */
class CacheCommon {
public:

	/**
	 * Method to be set as GDALs error handler. Swallaws all output.
	 */
	static void GDALErrorHandler(CPLErr eErrClass, int err_no, const char *msg);

	/**
	 * @return the time since epoch in ms.
	 */
	static time_t time_millis();

	/**
	 * Installs a custom uncaught exception handler which
	 * prints the stack-trace before terminating
	 */
	static void set_uncaught_exception_handler();

	/**
	 * @return the stacktrace of the last 20 calls
	 */
	static std::string get_stacktrace();

	/**
	 * Creates a listening socket on the given port
	 * @param port the port to listen on
	 * @paran nonblock whether the calls to accept are blocking
	 * @param backlog the backlog
	 */
	static int get_listening_socket(int port, bool nonblock = true, int backlog = 10);

	/**
	 * @return a string-representation for the given query-rectange
	 */
	static std::string qr_to_string( const QueryRectangle &rect );

	/**
	 * @return a string-representation for the given spatio-temporal reference
	 */
	static std::string stref_to_string( const SpatioTemporalReference &ref );

	/**
	 * @return if the resolution of two grid-results match (e.g. for puzzling)
	 */
	static bool resolution_matches( const GridSpatioTemporalResult &r1, const GridSpatioTemporalResult &r2 );

	/**
	 * @return if the resolution of two cache-cubes match (e.g. for puzzling)
	 */
	static bool resolution_matches( const CacheCube &c1, const CacheCube &c2 );

	/**
	 * @return if the given resolutions match (e.g. for puzzling)
	 */
	static bool resolution_matches( double scalex1, double scaley1, double scalex2, double scaley2 );

private:
	CacheCommon() {};
	~CacheCommon() {};
};

#endif /* COMMON_H_ */
