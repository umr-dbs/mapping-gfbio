
#include "services/httpservice.h"
#include "util/exceptions.h"
#include "util/debug.h"

#include <map>
#include <unordered_map>
#include <memory>
#include <algorithm>

#include <uriparser/Uri.h>



// The magic of type registration, see REGISTER_SERVICE in service.h
typedef std::unique_ptr<HTTPService> (*ServiceConstructor)();

static std::unordered_map< std::string, ServiceConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, ServiceConstructor > registered_constructors;
	return &registered_constructors;
}

HTTPServiceRegistration::HTTPServiceRegistration(const char *name, ServiceConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}


std::unique_ptr<HTTPService> HTTPService::getRegisteredService(const std::string &name) {
	auto map = getRegisteredConstructorsMap();
	auto it = map->find(name);
	if (it == map->end())
		throw ArgumentException(concat("No service named ", name, " is registered"));

	auto constructor = it->second;

	auto ptr = constructor();
	return ptr;
}


void HTTPService::run(const char *query_string, std::streambuf *out, std::streambuf *err) {
	std::ostream error(err);
	HTTPResponseStream response(out);
	try {
		if (!query_string)
			response.send500("no QUERY_STRING found");

		// Parse all entries
		HTTPService::Params params = parseQueryString(query_string);

		// TODO: port the client to WFS, remove FAKEWFS
		auto servicename = params.get("service", "FAKEWFS");
		auto service = HTTPService::getRegisteredService(servicename);

		service->run(params, response, error);
	}
	catch (const std::exception &e) {
		error << "Request failed with an exception: " << e.what() << "\n";
		response.send500("invalid request");
	}
}

/*
 * Query string parsing
 */
static char hexvalue(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return 0;
}

static std::string urldecode(const char *string) {
	const int len = strlen(string);

	std::vector<char> buffer;
	buffer.reserve(len);

	int pos = 0;
	while (pos < len) {
		const char c = string[pos];
		if (c == '%' && pos + 2 < len) {
			char out = 16 * hexvalue(string[pos+1]) + hexvalue(string[pos+2]);
			buffer.push_back(out);
			pos+=3;
		}
		else {
			buffer.push_back(c);
			pos++;
		}
	}

	return std::string(buffer.begin(), buffer.end());
}

HTTPService::Params HTTPService::parseQueryString(const char *query_string) {
	Params query_params;

	UriQueryListA *queryList;
	int itemCount;

	if (uriDissectQueryMallocA(&queryList, &itemCount, query_string, &query_string[strlen(query_string)]) != URI_SUCCESS)
		throw ArgumentException("Malformed Query String");

	UriQueryListA *item = queryList;
	while (item) {
		std::string key(item->key);
		std::transform(key.begin(), key.end(), key.begin(), ::tolower);

		std::string value = urldecode(item->value);
		if (key == "subset" || key == "size") {
			auto pos = value.find(',');
			if (pos != std::string::npos) {
				key = key + '_' + value.substr(0, pos);
				value = value.substr(pos+1);
			}
		}
		query_params[key] = value;
		//query_params.insert( std::make_pair( std::string(item->key), std::string(item->value) ) );
		item = item->next;
	}

	uriFreeQueryListA(queryList);

	return query_params;
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
	auto it = find(key);
	if (it == end())
		throw ArgumentException(concat("No parameter with key ", key));

	// TODO: throw exception on invalid values like "42abc"
	return std::stoi(it->second);
}

int HTTPService::Params::getInt(const std::string &key, int defaultvalue) const {
	auto it = find(key);
	if (it == end())
		return defaultvalue;

	return std::stoi(it->second);
}

bool HTTPService::Params::getBool(const std::string &key) const {
	return get(key) == "1";
}

bool HTTPService::Params::getBool(const std::string &key, bool defaultvalue) const {
	auto it = find(key);
	if (it == end())
		return defaultvalue;
	return it->second == "1";
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
