
#include "services/httpservice.h"
#include "services/httpparsing.h"
#include "util/exceptions.h"
#include "util/debug.h"
#include "util/configuration.h"

#include <map>
#include <unordered_map>
#include <memory>
#include <algorithm>



// The magic of type registration, see REGISTER_SERVICE in httpservice.h
typedef std::unique_ptr<HTTPService> (*ServiceConstructor)(const HTTPService::Params& params, HTTPService::HTTPResponseStream& result, std::ostream &error);

static std::unordered_map< std::string, ServiceConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, ServiceConstructor > registered_constructors;
	return &registered_constructors;
}

HTTPServiceRegistration::HTTPServiceRegistration(const char *name, ServiceConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}


std::unique_ptr<HTTPService> HTTPService::getRegisteredService(const std::string &name, const Params& params, HTTPResponseStream& result, std::ostream &error) {
	auto map = getRegisteredConstructorsMap();
	auto it = map->find(name);
	if (it == map->end())
		throw ArgumentException(concat("No service named ", name, " is registered"));

	auto constructor = it->second;

	auto ptr = constructor(params, result, error);
	return ptr;
}

HTTPService::HTTPService(const Params& params, HTTPResponseStream& result, std::ostream &error)
	: params(params), result(result), error(error) {
}

void HTTPService::run(std::streambuf *in, std::streambuf *out, std::streambuf *err) {
	std::istream input(in);
	std::ostream error(err);
	HTTPResponseStream response(out);
	try {
		// Parse all entries
		HTTPService::Params params;
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
}


/*
 * Service::Params
 */
std::string HTTPService::Params::get(const std::string &key) const {
	auto it = find(key);
	if (it == end())
		throw ArgumentException(concat("No parameter with key ", key));
	return it->second;
}

std::string HTTPService::Params::get(const std::string &key, const std::string &defaultvalue) const {
	auto it = find(key);
	if (it == end())
		return defaultvalue;
	return it->second;
}

int HTTPService::Params::getInt(const std::string &key) const {
	return Configuration::parseInt(get(key));
}

int HTTPService::Params::getInt(const std::string &key, int defaultvalue) const {
	auto it = find(key);
	if (it == end())
		return defaultvalue;
	return Configuration::parseInt(it->second);
}

bool HTTPService::Params::getBool(const std::string &key) const {
	return Configuration::parseBool(get(key));
}

bool HTTPService::Params::getBool(const std::string &key, bool defaultvalue) const {
	auto it = find(key);
	if (it == end())
		return defaultvalue;
	return Configuration::parseBool(it->second);
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
	auto msgs = get_debug_messages();
	*this << "Profiling-header: ";
	for (auto &str : msgs) {
		*this << str.c_str() << ", ";
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
