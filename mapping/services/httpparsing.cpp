#include "services/httpparsing.h"
#include "util/exceptions.h"

#include <string>
#include <vector>
#include <algorithm>

/**
 * std::string wrapper for getenv(). Throws exceptions if environment variable is not set.
 */
static std::string getenv_str(const std::string& varname) {
	const char* val = getenv(varname.c_str());

	if (!val)
		throw ArgumentException(concat("Invalid HTTP request, missing environment variable ", varname));

	return val;
}

/*
 * Converts a hex value to the corresponding character representation.
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

static void trim(std::string& str) {
	str.erase(0, str.find_first_not_of(" \n\r\t"));
	str.erase(str.find_last_not_of(" \n\r\t")+1);
}

/**
 * Decodes an URL
 */
static bool ishex(char c) {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

static std::string urldecode(const std::string& str) {
	const int len = str.length();

	std::vector<char> buffer;
	buffer.reserve(len);

	int pos = 0;
	while (pos < len) {
		const char c = str[pos];
		if (c == '%' && pos + 2 < len && ishex(str[pos+1]) && ishex(str[pos+2])) {
			char out = 16 * hexvalue(str[pos + 1]) + hexvalue(str[pos + 2]);
			buffer.push_back(out);
			pos += 3;
		} else {
			buffer.push_back(c);
			pos++;
		}
	}

	return std::string(buffer.begin(), buffer.end());
}


/**
 * Gets the data from a POST request
 */
static std::string getPostData(std::istream& in) {
	std::string content_length = getenv_str("CONTENT_LENGTH");

	int cl = std::stoi(content_length);
	if (cl < 0)
		throw ArgumentException("CONTENT_LENGTH is negative");

	char *buf = new char[cl];
	in.read(buf, cl);
	std::string query(buf, buf + cl);
	delete[] buf;
	return query;
}

static void parseQueryKeyValuePair(const std::string& q, std::map<std::string, std::string>& kvp) {
	std::string::size_type sep = q.find_first_of("=");

	std::string key, val;

	if(sep == std::string::npos) {
		key = q;
		val = ""; // Empty value
	}
	else {
		key = q.substr(0, sep);
		val = q.substr(sep+1, q.length() - (sep+1));
	}

	trim(key);
	if(key.empty())
		return;
	std::transform(key.begin(), key.end(), key.begin(), ::tolower);
	kvp[key] = val;
}

/**
 * Parses a query string into a given Params structure.
 */
static void parseQuery(const std::string& q, HTTPService::Params &params) {
	if (q.length() == 0)
		return;

	// see RFC 3986 ch. 3.4

	/*
	// query string is already extracted, no need for this part.
	auto beg = q.find_first_of("?");
	if(beg == std::string::npos) // No query string present
		return;
	auto end = q.find_first_of("#", beg);
	if(end == std::string::npos) // No fragment
		end = q.length() - 1;
		*/

	std::string::size_type last = 0;
	std::string::size_type cur;

	while( (cur = q.find_first_of("&", last)) != std::string::npos) {
		std::string sub = q.substr(last, cur-last);
		parseQueryKeyValuePair(sub, params);
		last = cur+1;
	}

	if(last < q.length()) {
		std::string sub = q.substr(last, q.length()-last);
		parseQueryKeyValuePair(sub, params);
	}
}

/**
 * Parses a url encoded POST request
 */
static void parsePostUrlEncoded(HTTPService::Params &params, std::istream &in) {
	std::string query = urldecode(getPostData(in));
	parseQuery(query, params);
}

/**
 * Parses a multipart POST request
 */
static void parsePostMultipart(HTTPService::Params &params, std::istream &in) {

	/*
	 // Force to check the boundary string length to defense overflow attack
	 const std::string boundary_str("--");
	 std::size_t maxboundarylen = boundary_str.length();
	 std::string content_type = getenv_str("CONTENT_TYPE");
	 maxboundarylen += ( content_type.length() - content_type.find("content_type=") ) + "content_type=";
	 maxboundarylen += boundary_str + "\r\n";
	 if(maxboundarylen >= )*/
}

/**
 * Parses POST data from a HTTP request
 */
void parsePostData(HTTPService::Params &params, std::istream &in) {
	std::string request_method = getenv_str("REQUEST_METHOD");

	if (request_method != "POST")
		return;

	std::string content_type = getenv_str("CONTENT_TYPE");
	if (content_type == "application/x-www-form-urlencoded") {
		parsePostUrlEncoded(params, in);
	} else if (content_type == "multipart/form-data") {
		parsePostMultipart(params, in);
	} else
		throw ArgumentException("Unknown content type in POST request.");
}

/**
 * Parses GET data from a HTTP request
 */
void parseGetData(HTTPService::Params &params) {
	std::string query_string = getenv_str("QUERY_STRING");
	query_string = urldecode(query_string);

	parseQuery(query_string, params);
}
