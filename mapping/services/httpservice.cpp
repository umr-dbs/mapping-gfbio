
#include "services/httpservice.h"
#include "services/httpparsing.h"
#include "util/exceptions.h"
#include "util/configuration.h"
#include "util/log.h"

#include <map>
#include <unordered_map>
#include <memory>
#include <algorithm>



// The magic of type registration, see REGISTER_SERVICE in httpservice.h
typedef std::unique_ptr<HTTPService> (*ServiceConstructor)(const Parameters& params, HTTPService::HTTPResponseStream& response, std::ostream &error);

static std::unordered_map< std::string, ServiceConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, ServiceConstructor > registered_constructors;
	return &registered_constructors;
}

HTTPServiceRegistration::HTTPServiceRegistration(const char *name, ServiceConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}


std::unique_ptr<HTTPService> HTTPService::getRegisteredService(const std::string &name, const Parameters& params, HTTPResponseStream& response, std::ostream &error) {
	auto map = getRegisteredConstructorsMap();
	auto it = map->find(name);
	if (it == map->end())
		throw ArgumentException(concat("No service named ", name, " is registered"));

	auto constructor = it->second;

	auto ptr = constructor(params, response, error);
	return ptr;
}

HTTPService::HTTPService(const Parameters& params, HTTPResponseStream& response, std::ostream &error)
	: params(params), response(response), error(error) {
}

void HTTPService::run(std::streambuf *in, std::streambuf *out, std::streambuf *err) {
	std::istream input(in);
	std::ostream error(err);
	HTTPResponseStream response(out);

	Log::logToStream(Log::LogLevel::WARN, &error);
	Log::logToMemory(Log::LogLevel::INFO);
	try {
		// Parse all entries
		Parameters params;
		parseGetData(params);
		parsePostData(params, input);

		auto servicename = params.get("service");
		auto service = HTTPService::getRegisteredService(servicename, params, response, error);

		service->run();
	}
	catch (const std::exception &e) {
		error << "Request failed with an exception: " << e.what() << "\n";
		response.send500("invalid request");
	}
	Log::off();
}


/*
 * Service::ResponseStream
 */
HTTPService::HTTPResponseStream::HTTPResponseStream(std::streambuf *buf) : std::ostream(buf), headers_sent(false) {
}

HTTPService::HTTPResponseStream::~HTTPResponseStream() {
}

void HTTPService::HTTPResponseStream::send500(const std::string &message) {
	sendHeader("Status", "500 Internal Server Error");
	sendContentType("text/plain");
	finishHeaders();
	*this << message;
}

void HTTPService::HTTPResponseStream::sendHeader(const std::string &key, const std::string &value) {
	*this << key << ": " << value << "\r\n";
}
void HTTPService::HTTPResponseStream::sendContentType(const std::string &contenttype) {
	sendHeader("Content-type", contenttype);
}

void HTTPService::HTTPResponseStream::sendDebugHeader() {
	const auto &msgs = Log::getMemoryMessages();
	*this << "Profiling-header: ";
	for (const auto &str : msgs) {
		*this << str << ", ";
	}
	*this << "\r\n";
}

void HTTPService::HTTPResponseStream::finishHeaders() {
	*this << "\r\n";
	headers_sent = true;
}
bool HTTPService::HTTPResponseStream::hasSentHeaders() {
	return headers_sent;
}


void HTTPService::HTTPResponseStream::sendJSON(const Json::Value &obj) {
	sendContentType("application/json; charset=utf-8");
	sendDebugHeader();
	finishHeaders();
	*this << obj;
}

void HTTPService::HTTPResponseStream::sendSuccessJSON(Json::Value &obj) {
	obj["result"] = true;
	sendJSON(obj);
}

void HTTPService::HTTPResponseStream::sendSuccessJSON() {
	Json::Value obj(Json::ValueType::objectValue);
	sendSuccessJSON(obj);
}

void HTTPService::HTTPResponseStream::sendFailureJSON(const std::string &error) {
	Json::Value obj(Json::ValueType::objectValue);
	obj["result"] = error;
	sendJSON(obj);
}
