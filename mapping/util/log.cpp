/*
 * Log.cpp
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#include "util/log.h"
#include <thread>
#include <sstream>
#include <iomanip>

void Log::setLogFd(FILE *fd) {
	Log::fd = fd;
}
void Log::setLevel(LogLevel level) {
	Log::level = level;
}

void Log::error(const char* msg, ...) {
	va_list arglist;
	va_start(arglist, msg);
	Log::log(LogLevel::ERROR, msg, arglist);
	va_end(arglist);
}

void Log::warn(const char* msg, ...) {
	va_list arglist;
	va_start(arglist, msg);
	Log::log(LogLevel::WARN, msg, arglist);
	va_end(arglist);
}

void Log::info(const char* msg, ...) {
	va_list arglist;
	va_start(arglist, msg);
	Log::log(LogLevel::INFO, msg, arglist);
	va_end(arglist);
}

void Log::debug(const char* msg, ...) {
	va_list arglist;
	va_start(arglist, msg);
	Log::log(LogLevel::DEBUG, msg, arglist);
	va_end(arglist);
}

void Log::trace(const char* msg, ...) {
	va_list arglist;
	va_start(arglist, msg);
	Log::log(LogLevel::TRACE, msg, arglist);
	va_end(arglist);
}

#ifndef DISABLE_LOGGING

void Log::log(LogLevel level, const char *msg, va_list vargs) {
	if (level <= Log::level) {
		std::ostringstream ss;

		// Time
		time_t now = time(0);
		struct tm tstruct;
		char buf[80];
		tstruct = *localtime(&now);
		strftime(buf, sizeof(buf), "%F %H:%M:%S.", &tstruct);


		ss << "[" << buf << std::setfill('0') << std::setw(20) << (now % 1000) << "] [";

		// level
		switch (level) {
		case LogLevel::TRACE:
			ss << "TRACE";
			break;
		case LogLevel::DEBUG:
			ss << "DEBUG";
			break;
		case LogLevel::INFO:
			ss << "INFO ";
			break;
		case LogLevel::WARN:
			ss << "WARN ";
			break;
		case LogLevel::ERROR:
			ss << "ERROR";
			break;
		default:
			ss << "UNKNW";
		}

		// Thread-ID

		ss << "] [" << std::this_thread::get_id() << "] " << msg << "\n";

		// Do the actual logging
		vfprintf(fd, ss.str().c_str(), vargs);
	}
}
#endif
#ifdef DISABLE_LOGGING
void Log::log(LogLevel level, const char *msg, ...) {}
#endif

Log::LogLevel Log::level = LogLevel::DEBUG;
FILE* Log::fd = stderr;
