
#include "services/httpparsing.h"
#include "util/exceptions.h"

#include <uriparser/Uri.h>
#include <string>
#include <vector>
#include <algorithm>

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

void parseGetData(HTTPService::Params &params) {
	const char *query_string = getenv("QUERY_STRING");
	if (!query_string)
		throw ArgumentException("QUERY_STRING not set");

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
		params[key] = value;
		//query_params.insert( std::make_pair( std::string(item->key), std::string(item->value) ) );
		item = item->next;
	}

	uriFreeQueryListA(queryList);
}


void parsePostData(HTTPService::Params &params, std::istream &in) {
	// TODO
}
