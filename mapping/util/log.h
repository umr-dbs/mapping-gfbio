/*
 * Log.h
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#ifndef LOG_H_
#define LOG_H_
//#define DISABLE_LOGGING

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

enum LogLevel {
	ERROR = 1, WARN = 2, INFO = 3, DEBUG = 4, TRACE = 5
};

class Log {
public:
	static void setLogFd(FILE *fd);
	static void setLevel(LogLevel level);
	static void log(LogLevel level, const char *msg, ...);
private:
	static LogLevel level;
	static FILE *fd;
};

#endif /* LOG_H_ */
