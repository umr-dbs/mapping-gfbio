/*
 * Log.cpp
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#include "util/log.h"
#include <stdio.h>
#include <stdarg.h>
#include <ctime>
#include <string>
#include <thread>
#include <sstream>

void Log::setLogFd(FILE *fd) {
	Log::fd = fd;
}
void Log::setLevel(LogLevel level) {
	Log::level = level;
}

#ifndef DISABLE_LOGGING
void Log::log(LogLevel level, const char *msg, ...) {
	if (level <= Log::level) {
		std::ostringstream ss;

		// Time
		time_t     now = time(0);
		struct tm  tstruct;
		char       buf[80];
		tstruct = *localtime(&now);
		strftime(buf, sizeof(buf), "%F %H:%M:%S.", &tstruct);

		ss << "[" << buf << (now%1000) << "] [";

		// level
		switch ( level ) {
			case TRACE: ss << "TRACE"; break;
			case DEBUG: ss << "DEBUG"; break;
			case INFO:  ss << "INFO "; break;
			case WARN:  ss << "WARN "; break;
			case ERROR: ss << "ERROR"; break;
			default:    ss << "UNKNW";
		}

		// Thread-ID

		ss << "] [" << std::this_thread::get_id() << "] " << msg << "\n";

		// Do the actual logging
		va_list arglist;
		va_start(arglist, msg);
		vfprintf(fd, ss.str().c_str(), arglist);
		va_end(arglist);
	}
}
#endif
#ifdef DISABLE_LOGGING
void Log::log(LogLevel level, const char *msg, ...) {}
#endif

LogLevel Log::level = INFO;
FILE *Log::fd = stderr;
