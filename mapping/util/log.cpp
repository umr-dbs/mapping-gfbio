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
	if      ( lower == "trace" ) setLevel(LogLevel::TRACE);
	else if ( lower == "debug" ) setLevel(LogLevel::DEBUG);
	else if ( lower == "info" ) setLevel(LogLevel::INFO);
	else if ( lower == "warn" ) setLevel(LogLevel::WARN);
	else if ( lower == "error" ) setLevel(LogLevel::ERROR);
	else throw ArgumentException(concat("Illegal LogLevel: ",lower));
}

#ifndef DISABLE_LOGGING

void Log::error(const char* msg, ...) {
	if ( LogLevel::ERROR > Log::level )
		return;
	va_list arglist;
	va_start(arglist, msg);
	Log::log("ERROR", msg, arglist);
	va_end(arglist);
}

void Log::warn(const char* msg, ...) {
	if ( LogLevel::WARN > Log::level )
		return;
	va_list arglist;
	va_start(arglist, msg);
	Log::log("WARN ", msg, arglist);
	va_end(arglist);
}

void Log::info(const char* msg, ...) {
	if ( LogLevel::INFO > Log::level )
		return;
	va_list arglist;
	va_start(arglist, msg);
	Log::log("INFO ", msg, arglist);
	va_end(arglist);
}

void Log::debug(const char* msg, ...) {
	if ( LogLevel::DEBUG > Log::level )
		return;
	va_list arglist;
	va_start(arglist, msg);
	Log::log("DEBUG", msg, arglist);
	va_end(arglist);
}

void Log::trace(const char* msg, ...) {
	if ( LogLevel::TRACE > Log::level )
		return;
	va_list arglist;
	va_start(arglist, msg);
	Log::log("TRACE", msg, arglist);
	va_end(arglist);
}

void Log::trace_time( const std::string &msg ) {
	va_list arglist;
	Log::log("TIME ", msg.c_str(), arglist );
}

void Log::log(const std::string &level, const char *msg, va_list vargs) {
	std::ostringstream ss;

	// Time
	auto tp = std::chrono::system_clock::now();
	auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	time_t now = std::chrono::system_clock::to_time_t(tp);
	struct tm tstruct;
	char buf[80];
	tstruct = *localtime(&now);
	strftime(buf, sizeof(buf), "%F %H:%M:%S.", &tstruct);


	ss << "[" << buf << std::setfill('0') << std::setw(3) << (millis % 1000) << "] ";
	ss << "[" << level << "] ";
	// Thread-ID

	ss << "[" << std::this_thread::get_id() << "] " << msg << std::endl;

	// Do the actual logging
	vfprintf(fd, ss.str().c_str(), vargs);
}
#endif
#ifdef DISABLE_LOGGING
void Log::error(const char* msg, ...) {}
void Log::warn(const char* msg, ...) {}
void Log::info(const char* msg, ...) {}
void Log::debug(const char* msg, ...) {}
void Log::trace(const char* msg, ...) {}
void Log::trace_time( const std::string &msg ) {}
void Log::log(const std::string &level, const char *msg, va_list vargs) {}
#endif

Log::LogLevel Log::level = LogLevel::WARN;
FILE* Log::fd = stderr;
