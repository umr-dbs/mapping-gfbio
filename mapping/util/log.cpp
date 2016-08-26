/*
 * Log.cpp
 *
 *  Created on: 15.05.2015
 *      Author: mika
 */

#include "util/log.h"
#include "util/concat.h"
#include "util/exceptions.h"
#include "util/enumconverter.h"

#include <thread>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <mutex>


/*
 * LogLevel enum
 */
const std::vector< std::pair<Log::LogLevel, std::string> > LogLevelMap {
	std::make_pair(Log::LogLevel::OFF, "OFF"),
	std::make_pair(Log::LogLevel::ERROR, "ERROR"),
	std::make_pair(Log::LogLevel::WARN, "WARN"),
	std::make_pair(Log::LogLevel::INFO, "INFO"),
	std::make_pair(Log::LogLevel::DEBUG, "DEBUG"),
	std::make_pair(Log::LogLevel::TRACE, "TRACE")
};

EnumConverter<Log::LogLevel> LogLevelConverter(LogLevelMap);

/*
 * Static logging functions
 */
static Log::LogLevel maxLogLevel = Log::LogLevel::OFF;

static std::vector<std::string> memorylog;
static Log::LogLevel memorylog_level = Log::LogLevel::OFF;

static std::ostream *streamlog = nullptr;
static Log::LogLevel streamlog_level = Log::LogLevel::OFF;

static std::mutex log_mutex;


static void log(Log::LogLevel level, const std::string &msg) {
	// avoid assembling the message unless it is really needed
	if (level > maxLogLevel)
		return;

	// Calculate the timestamp
	auto tp = std::chrono::system_clock::now();
	auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	time_t now = std::chrono::system_clock::to_time_t(tp);
	auto tstruct = localtime(&now);
	char buf[80];
	strftime(buf, sizeof(buf), "%F %H:%M:%S.", tstruct);

	// assemble the message
	std::ostringstream ss;
	ss << "[" << buf << std::setfill('0') << std::setw(3) << (millis % 1000) << "] ";
	ss << "[" << LogLevelConverter.to_string(level) << "] ";
	ss << "[" << std::hex << std::this_thread::get_id() << "] " << msg;

	std::string message = ss.str();
	// Do the actual logging
	std::lock_guard<std::mutex> guard(log_mutex);
	if (level <= memorylog_level)
		memorylog.push_back(message);
	if (level <= streamlog_level && streamlog)
		(*streamlog) << message << std::endl;
}

static Log::LogLevel levelFromString(const std::string &level) {
	std::string upper;
	upper.resize( level.size() );
	std::transform(level.begin(),level.end(),upper.begin(),::toupper);
	return LogLevelConverter.from_string(upper);
}

static std::string sprintf(const char *msg, va_list arglist) {
	va_list arglist2;
	va_copy(arglist2, arglist);

	auto len = std::vsnprintf(nullptr, 0, msg, arglist);
	len = std::max(1, len+1);
	char *result = new char[len];
	std::vsnprintf(result, len, msg, arglist2);
	va_end(arglist2);

	std::string str(result);
	delete(result);
	return str;
}


/*
 * Initialize the logging
 */
void Log::logToStream(const std::string &level, std::ostream *stream) {
	logToStream(levelFromString(level), stream);
}
void Log::logToStream(LogLevel level, std::ostream *stream) {
	std::lock_guard<std::mutex> guard(log_mutex);
	streamlog_level = level;
	streamlog = stream;
	maxLogLevel = std::max(memorylog_level, streamlog_level);
}

void Log::logToMemory(const std::string &level) {
	logToMemory(levelFromString(level));
}

void Log::logToMemory(LogLevel level) {
	std::lock_guard<std::mutex> guard(log_mutex);
	memorylog_level = level;
	maxLogLevel = std::max(memorylog_level, streamlog_level);
}

std::vector<std::string> Log::getMemoryMessages() {
	std::lock_guard<std::mutex> guard(log_mutex);
	std::vector<std::string> result;
	std::swap(memorylog, result);
	return result;
}

void Log::off() {
	std::lock_guard<std::mutex> guard(log_mutex);
	memorylog_level = LogLevel::OFF;
	memorylog.clear();
	streamlog_level = LogLevel::OFF;
	streamlog = nullptr;
	maxLogLevel = LogLevel::OFF;
}

/*
 * Implement the actual loglevels
 */
void Log::error(const char* msg, ...) {
	if ( LogLevel::ERROR > maxLogLevel )
		return;
	va_list arglist;
	va_start(arglist, msg);
	auto smsg = sprintf(msg, arglist);
	va_end(arglist);
	error(smsg);
}
void Log::error(const std::string &msg) {
	log(LogLevel::ERROR, msg);
}

void Log::warn(const char* msg, ...) {
	if ( LogLevel::WARN > maxLogLevel )
		return;
	va_list arglist;
	va_start(arglist, msg);
	auto smsg = sprintf(msg, arglist);
	va_end(arglist);
	warn(smsg);
}
void Log::warn(const std::string &msg) {
	log(LogLevel::WARN, msg);
}

void Log::info(const char* msg, ...) {
	if ( LogLevel::INFO > maxLogLevel )
		return;
	va_list arglist;
	va_start(arglist, msg);
	auto smsg = sprintf(msg, arglist);
	va_end(arglist);
	info(smsg);
}
void Log::info(const std::string &msg) {
	log(LogLevel::INFO, msg);
}

void Log::debug(const char* msg, ...) {
	if ( LogLevel::DEBUG > maxLogLevel )
		return;
	va_list arglist;
	va_start(arglist, msg);
	auto smsg = sprintf(msg, arglist);
	va_end(arglist);
	debug(smsg);
}
void Log::debug(const std::string &msg) {
	log(LogLevel::DEBUG, msg);
}

void Log::trace(const char* msg, ...) {
	if ( LogLevel::TRACE > maxLogLevel )
		return;
	va_list arglist;
	va_start(arglist, msg);
	auto smsg = sprintf(msg, arglist);
	va_end(arglist);
	trace(smsg);
}
void Log::trace(const std::string &msg) {
	log(LogLevel::TRACE, msg);
}
