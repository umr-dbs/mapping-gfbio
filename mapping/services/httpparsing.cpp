#include "services/httpparsing.h"
#include "util/exceptions.h"

#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include "util/make_unique.h"
#include "util/base64.h"
#include "util/configuration.h"

/**
 * std::string wrapper for getenv(). Throws exceptions if environment variable is not set.
 */
static std::string getenv_str(const std::string& varname, bool to_lower = false) {
	const char* val = getenv(varname.c_str());

	if (!val)
		throw ArgumentException(concat("Invalid HTTP request, missing environment variable ", varname));

	std::string result = val;
	if (to_lower)
		std::transform(result.begin(), result.end(), result.begin(), ::tolower);
	return result;
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

static void trim(std::string& str, bool left = true, bool right = true, const std::string& delimiters = " \n\r\t") {
	if (left)
		str.erase(0, str.find_first_not_of(delimiters));
	if (right)
		str.erase(str.find_last_not_of(delimiters) + 1);
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
		if (c == '%' && pos + 2 < len && ishex(str[pos + 1]) && ishex(str[pos + 2])) {
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

static void parseKeyValuePair(const std::string& q, std::map<std::string, std::string>& kvp, bool unescape = false, const std::string& delim = "=") {
	std::string::size_type sep = q.find_first_of(delim);

	std::string key, val;

	if (sep == std::string::npos) {
		key = q;
		val = ""; // Empty value
	} else {
		key = q.substr(0, sep);
		val = q.substr(sep + 1, q.length() - (sep + 1));
		trim(val);
	}

	trim(key);
	if (key.empty())
		return;
	std::transform(key.begin(), key.end(), key.begin(), ::tolower);

	if (unescape && val.size() >= 2) {

		if (val.front() == '\"' && val.back() == '\"') {
			val.erase(0, 1);
			val.erase(val.length() - 1);
		}
	}

	kvp[key] = val;
}

/**
 * Parses a query string into a given Params structure.
 */
void parseQuery(const std::string& query, Parameters &params) {
	if (query.length() == 0)
		return;

	auto q = urldecode(query);

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

	while ((cur = q.find_first_of("&", last)) != std::string::npos) {
		std::string sub = q.substr(last, cur - last);
		parseKeyValuePair(sub, params);
		last = cur + 1;
	}

	if (last < q.length()) {
		std::string sub = q.substr(last, q.length() - last);
		parseKeyValuePair(sub, params);
	}
}

/**
 * Parses a url encoded POST request
 */
static void parsePostUrlEncoded(Parameters &params, std::istream &in) {
	std::string query = getPostData(in);
	parseQuery(query, params);
}

bool parseMultipartParameter(const std::string& line, std::map<std::string, std::string>& params) {
	std::string::size_type delim = line.find_first_of(":");
	if (delim != std::string::npos) {
		std::string param = line.substr(0, delim);
		trim(param);
		std::transform(param.begin(), param.end(), param.begin(), tolower);
		if (param == "content-type" || param == "content-disposition" || param == "content-transfer-encoding") {
			std::string val = line.substr(delim + 1);
			trim(val);
			if (param == "content-transfer-encoding") {
				std::transform(val.begin(), val.end(), val.begin(), tolower);
			}
			params[param] = val;
			return true;
		}
	}
	return false;
}

static void parseMultipartSubBoundary(Parameters &params, std::istream& in, std::map<std::string, std::string>& pars);

static void parseMultipartBoundary(Parameters &params, std::istream& in, const std::string& boundary) {

	const std::string boundary_start = "--" + boundary;
	const std::string boundary_end = boundary_start + "--";

	// Begin parsing the boundaries. Since the RFC does not mention the explicit requirement of having line-breaks between
	// the boundary tokens (--boundary,--boundary--), this is done by character comparison.

	std::size_t match_size = 0;
	std::string buf;

	bool found_end = false;
	bool in_boundary = false;
	while (in.good() && !in.eof()) {
		char c = in.get();
		buf.push_back(c);
		if (boundary_start.length() > match_size) {
			// Seek the start
			if (boundary_start[match_size] == c) {
				match_size++;

			} else
				match_size = 0;
			if (match_size == boundary_start.length()) {
				// Found boundary start
				if (!buf.empty() && in_boundary) {
					buf.erase(buf.length() - boundary_start.length()); // Cut the boundary string
					std::stringstream sub_boundary_stream;
					sub_boundary_stream << buf;
					std::map<std::string, std::string> pars;
					parseMultipartSubBoundary(params, sub_boundary_stream, pars);
				}
				in_boundary = true;
				buf.clear();
			}
		} else if (boundary_end.length() > match_size) {
			// Seek the start
			if (boundary_end[match_size] == c) {
				match_size++;
			} else
				match_size = 0;
			if (match_size == boundary_end.length()) {
				// Found boundary end, skip the epilogue
				found_end = true;
				break;
			}
		}
	}
	buf.clear();
	if (!found_end) {
		throw std::runtime_error("Unexpected end of stream.");
	}

}

static void parseMultipartSubBoundary(Parameters &params, std::istream& in, std::map<std::string, std::string>& pars) {

	// see RFC 1341 ch 7.2.1
	// This method may be used as recursive in order to support nested sub-boundaries.
	std::string body;
	std::vector<std::string> potential_body_lines;
	bool in_body = false;

	while (in.good() && !in.eof()) {
		std::string line;
		do { // std::getline doesn't seem to work here for some reason.
			char c=in.get();
			if(c < 0 || c == '\0' || c == '\n' || c=='\r') {
				break;
			}
			line += c;
		} while(in);


		// Parse parameters first, then body.
		if (!in_body && !parseMultipartParameter(line, pars)) {
			// We found a line that is not a parameter. Add the raw line to the potential body line buffer.
			// This guarantees that empty lines will be preserved as long as the body format supports this.
			potential_body_lines.push_back(line);
			if (line.find_first_not_of(" \n\r\t") != std::string::npos) {
				// We found a line that is neither a parameter nor an empty line. This guarantees we are in the body segment already.
				in_body = true;
			}

		} else {
			// Another parameter.
			if (!in_body) {
				// Clear the body line buffer if there are only empty lines.
				potential_body_lines.clear();
			} else
				potential_body_lines.push_back(line);
		}
	}

	// Build the body based on the parsed parameters
	for (auto& line : potential_body_lines) {
		const std::string& enc = pars["content-transfer-encoding"];
		if (enc == "base64") {
			trim(line);
			body += base64_decode(line);
		} else if (enc.empty()) { // Default behaviour
			body += line + "\n";
		} else
			throw ArgumentException("Unsupported transfer encoding.");
	}
	potential_body_lines.clear();

	// Now check if this is a multipart message
	{
		std::string boundary_start;
		std::string boundary_end;
		std::string::size_type cur, last = 0;

		std::string& params_str = pars["content-type"];
		std::map<std::string, std::string> cparams;

		while ((cur = params_str.find_first_of(";", last)) != std::string::npos) {
			std::string sub = params_str.substr(last, cur - last);
			if (last > 0) {
				parseKeyValuePair(sub, cparams, true);
			}
			last = cur + 1;
		}

		if (last < params_str.length()) {
			std::string sub = params_str.substr(last, params_str.length() - last);
			parseKeyValuePair(sub, cparams, true);
		}

		if (cparams.find("boundary") != cparams.end()) {
			std::string boundary = cparams["boundary"];
			trim(boundary, false, true); // RFC specifies only right whitespace removal.
			if (boundary.length() >= 70) // RFC explicitly restricts this to 70 characters.
				throw ArgumentException("Boundary length exceeded.");

			std::stringstream bodyStream;
			bodyStream << body;
			body.clear();
			parseMultipartBoundary(params, bodyStream, boundary);

			return;

		}

	}

	// If this is not a multipart message treat it as any regular post message (that may contain form data as well).

	const std::string& content_disposition = pars["content-disposition"];
	if(!content_disposition.empty()) {
		std::string::size_type last = 0;
		std::string::size_type cur;

		std::map<std::string, std::string> cdparams;
		std::string type;

		while ((cur = content_disposition.find_first_of(";", last)) != std::string::npos) {
			std::string sub = content_disposition.substr(last, cur - last);
			if(last == 0) {
				type = sub;
				std::transform(type.begin(), type.end(), type.begin(), tolower);
			}
			else
				parseKeyValuePair(sub, cdparams, true, "=");
			last = cur + 1;
		}

		if (last < content_disposition.length()) {
			std::string sub = content_disposition.substr(last, content_disposition.length() - last);
			parseKeyValuePair(sub, cdparams, true, "=");
		}

		if(type == "form-data") {

			if(cdparams["name"].empty()) {
				throw ArgumentException("Missing form-data name parameter.");
			}

			std::string name = cdparams["name"];
/*
			if(!cdparams["filename"].empty()) {

				// Extend HTTPService::Params with information about where to save (/tmp by default?)
				const std::string directory = Configuration::get("multipart_default_file_directory", "/tmp");
				std::string filename = directory + "/" +cdparams["filename"];

				std::ofstream of(filename);
				if(!of.good()) {
					throw ArgumentException("Cannot open file " + filename + " for writing");
				}

				of << body;
				of.close();

				params[name] = filename;
			}
			else */{
				// Store file in memory (string encoded into HTTPService::Params? What about binary data?)
				params[name] = body;

			}

		}
		else {
			throw ArgumentException("Unsupported content disposition type.");
		}
	}

}

/**
 * Parses a multipart POST request
 */
static void parsePostMultipart(Parameters &params, std::istream &in) {

	// This is merely a wrapper around parseMultipartSubBoundary() that will forward the
	// content type provided in the system environment variables.

	// The standard describes multipart messages can be nested, so the method below will
	// treat each single boundary (that is in fact considered a "part") as if it was sent un-nested.

	// Ultimately, this method will supersede parsePostData() at some time.

	// const std::streamsize max_message_length = 1024 * 1024 * 24; // Restrict the maximum message size
	std::map<std::string, std::string> pars;

	// HTTP header fields are always case-insensitive (RFC 2616 ch. 4.2)
	pars["content-type"] = getenv_str("CONTENT_TYPE", true);
	parseMultipartSubBoundary(params, in, pars);
	return;
}

/**
 * Parses POST data from a HTTP request
 */
void parsePostData(Parameters &params, std::istream &in) {

	// Methods are always case-sensitive (RFC 2616 ch. 5.1.1)
	std::string request_method = getenv_str("REQUEST_METHOD", false);

	if (request_method != "POST")
		return;

	// HTTP header fields are always case-insensitive (RFC 2616 ch. 4.2)
	std::string content_type = getenv_str("CONTENT_TYPE", true);

	if (content_type == "application/x-www-form-urlencoded") {
		parsePostUrlEncoded(params, in);
	} else if (content_type.find("multipart/form-data") != std::string::npos || content_type.find("multipart/mixed") != std::string::npos) {
		parsePostMultipart(params, in);
	} else
		throw ArgumentException("Unknown content type in POST request.");
}

/**
 * Parses GET data from a HTTP request
 */
void parseGetData(Parameters &params) {
	std::string query_string = getenv_str("QUERY_STRING");

	parseQuery(query_string, params);
}
