/*
 * Templates for mapping enums to strings and back.
 *
 * Mostly used for parameter parsing. Don't use it for enums with lots of values.
 *
 * For an example how to use this, see operators/points/csvpointsource.cpp
 */

#include "util/exceptions.h"

#include <string>
#include <tuple>
#include <array>
#include <vector>
#include <json/json.h>

template<typename T, std::size_t F, const std::array< std::pair<T, const char*>, F> &map	>
struct EnumConverter {
	EnumConverter() = delete;

	static const std::string &to_string(T t) {
		for (auto &tuple : map) {
			if (tuple.first == t)
				return tuple.second;
		}
		throw ArgumentException("No string found for enum value");
	}

	static const std::string &default_string() {
		return map.at(0).second;
	}

	static T from_string(const std::string &s) {
		for (auto &tuple : map) {
			if (tuple.second == s)
				return tuple.first;
		}
		throw ArgumentException(concat("No enum value found for identifier \"", s, "\""));
	}

	static T from_json(const Json::Value &root, const std::string &name) {
		auto str = root.get(name, default_string()).asString();
		return from_string(str);
	}
};
