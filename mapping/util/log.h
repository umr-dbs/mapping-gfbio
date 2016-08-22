/*
 * Log.h
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#ifndef UTIL_LOG_H_
#define UTIL_LOG_H_

#include <string>
#include <ostream>
#include <stdarg.h>

/**
 * Log functionality
 */
class Log {
public:
	enum class LogLevel {
		OFF, ERROR, WARN, INFO, DEBUG, TRACE
	};

	/**
	 * Logs to a stream, usually std::cerr
	 * There can only be one stream at a time. Calling this again will remove the previous stream.
	 *
	 * Note that it is the caller's responsibility to ensure that the lifetime of the stream is larger than the lifetime of the log.
	 */
	static void logToStream(const std::string &level, std::ostream *stream);
	static void logToStream(LogLevel level, std::ostream *stream);
	/**
	 * Enables logging to memory. It is possible to log both to a stream and to memory at the same time, even with different loglevels.
	 */
	static void logToMemory(const std::string &level);
	static void logToMemory(LogLevel level);
	/**
	 * Turns logging off.
	 */
	static void off();

	static void error(const char *msg, ...);
	static void error(const std::string &msg);
	static void warn(const char *msg, ...);
	static void warn(const std::string &msg);
	static void info(const char *msg, ...);
	static void info(const std::string &msg);
	static void debug(const char *msg, ...);
	static void debug(const std::string &msg);
	static void trace(const char *msg, ...);
	static void trace(const std::string &msg);
};

#endif /* LOG_H_ */
