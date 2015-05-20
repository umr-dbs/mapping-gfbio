/*
 * Log.cpp
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#include "cache/log.h"
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

void Log::log(LogLevel level, const char *msg, ...) {
	if (level <= Log::level) {

		// Time
		time_t     now = time(0);
		struct tm  tstruct;
		char       buf[80];
		tstruct = *localtime(&now);
		strftime(buf, sizeof(buf), "%F %H:%M:%S.", &tstruct);

		// level
		std::string levelstr = "TRACE";
		switch ( level ) {
			case DEBUG: levelstr = "DEBUG"; break;
			case INFO: levelstr  = "INFO"; break;
			case WARN: levelstr  = "WARN"; break;
			case ERROR: levelstr = "ERROR"; break;
			default: break;
		}

		// Thread-ID
		std::ostringstream ss;
		ss << std::this_thread::get_id();

		fprintf(fd, "[%s] [%-5s] [%-6s] ", buf, levelstr.c_str(), ss.str().c_str());
		//fprintf(fd, "[%s%3ld] [%-5s] ", buf, (now % 1000), levelstr.c_str());

		// Do the actual logging
		va_list arglist;
		va_start(arglist, msg);
		vfprintf(fd, msg, arglist);
		va_end(arglist);
		fprintf(fd, "\n");
	}
}

LogLevel Log::level = INFO;
FILE *Log::fd = stderr;
