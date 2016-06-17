#include "util/exceptions.h"

#include <string>
#include <tuple>
#include <vector>
#include <json/json.h>


/*
 * Templates for mapping enums to strings and back.
 *
 * Mostly used for parameter parsing. Don't use it for enums with lots of values.
 *
 * For an example how to use this, see operators/points/csvpointsource.cpp
 */
template<typename T>
class EnumConverter {
	public:
		EnumConverter(const std::vector< std::pair<T, std::string>> &map) : map(map) {};

		const std::string &to_string(T t) {
			for (auto &tuple : map) {
				if (tuple.first == t)
					return tuple.second;
			}
			throw ArgumentException("No string found for enum value");
		}

		const std::string &default_string() {
			return map.at(0).second;
		}

		T from_string(const std::string &s) {
			for (auto &tuple : map) {
				if (tuple.second == s)
					return tuple.first;
			}
			throw ArgumentException(concat("No enum value found for identifier \"", s, "\""));
		}

		T from_json(const Json::Value &root, const std::string &name) {
			auto str = root.get(name, default_string()).asString();
			return from_string(str);
		}
	private:
		const std::vector< std::pair<T, std::string>> &map;
};
