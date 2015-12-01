/*
 * Log.cpp
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#include "util/log.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include <thread>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>

void Log::setLogFd(FILE *fd) {
	Log::fd = fd;
}
void Log::setLevel(LogLevel level) {
	Log::level = level;
}

void Log::setLevel(const std::string& level) {
	std::string lower;
	lower.resize( level.size() );
	std::transform(level.begin(),level.end(),lower.begin(),::tolower);
	if      ( level == "trace" ) setLevel(LogLevel::TRACE);
	else if ( level == "debug" ) setLevel(LogLevel::DEBUG);
	else if ( level == "info" ) setLevel(LogLevel::INFO);
	else if ( level == "warn" ) setLevel(LogLevel::WARN);
	else if ( level == "error" ) setLevel(LogLevel::ERROR);
	else throw ArgumentException(concat("Illegal LogLevel",level));
}

#ifndef DISABLE_LOGGING

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

void Log::log(LogLevel level, const char *msg, va_list vargs) {
	if (level <= Log::level) {
		std::ostringstream ss;

		// Time
		auto tp = std::chrono::system_clock::now();
		auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
		time_t now = std::chrono::system_clock::to_time_t(tp);
		struct tm tstruct;
		char buf[80];
		tstruct = *localtime(&now);
		strftime(buf, sizeof(buf), "%F %H:%M:%S.", &tstruct);


		ss << "[" << buf << std::setfill('0') << std::setw(3) << (millis % 1000) << "] [";

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
void Log::error(const char* msg, ...) {}
void Log::warn(const char* msg, ...) {}
void Log::info(const char* msg, ...) {}
void Log::debug(const char* msg, ...) {}
void Log::trace(const char* msg, ...) {}
void Log::log(LogLevel level, const char *msg, va_list vargs) {}
#endif

Log::LogLevel Log::level = LogLevel::WARN;
FILE* Log::fd = stderr;
