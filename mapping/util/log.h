/*
 * Log.h
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#ifndef UTIL_LOG_H_
#define UTIL_LOG_H_
//#define DISABLE_LOGGING

#include <string>
#include <cstdio>
#include <stdarg.h>

/**
 * Log functionality
 */
class Log {
public:
	enum class LogLevel {
		OFF, ERROR, WARN, INFO, DEBUG, TRACE
	};

	static void setLogFd(FILE *fd);
	static void setLevel( const std::string &level );
	static void setLevel(LogLevel level);
	static void error(const char *msg, ...);
	static void warn(const char *msg, ...);
	static void info(const char *msg, ...);
	static void debug(const char *msg, ...);
	static void trace(const char *msg, ...);
private:
	static void trace_time( const std::string &msg );
	static void log(const std::string &level, const char *msg, va_list vargs);
	static LogLevel level;
	static FILE *fd;
	friend class ExecTimer;
};

#endif /* LOG_H_ */
