#include "datatypes/unit.h"
#include "util/exceptions.h"
#include "util/enumconverter.h"

#include <cstdio>
#include <cmath>
#include <string>
#include <limits>
#include <algorithm>
#include <json/json.h>


const std::vector< std::pair<Unit::Interpolation, std::string> > InterpolationSpecificationMap {
	std::make_pair(Unit::Interpolation::Unknown, "unknown"),
	std::make_pair(Unit::Interpolation::Discrete, "discrete"),
	std::make_pair(Unit::Interpolation::Continuous, "continuous")
};

EnumConverter<Unit::Interpolation> InterpolationConverter(InterpolationSpecificationMap);


static void str_to_lower(std::string &str) {
	std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

Unit::Unit(const std::string &json) {
	std::istringstream iss(json);
	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(iss, root) || !root.isObject())
		throw ArgumentException("Unit invalid: not a parseable json object");

	init(root);
}
Unit::Unit(const Json::Value &json) {
	init(json);
}

void Unit::init(const Json::Value &json) {
	measurement = json.get("measurement", "").asString();
	str_to_lower(measurement);
	unit = json.get("unit", "").asString();
	str_to_lower(unit);

	if (measurement.size() == 0 || unit.size() == 0)
		throw ArgumentException("Unit invalid: measurement or unit empty");

	min = json.get("min", -std::numeric_limits<double>::infinity()).asDouble();
	max = json.get("max", std::numeric_limits<double>::infinity()).asDouble();

	if (std::isnan(min) || std::isnan(max) || min >= max)
		throw ArgumentException("min or max not valid");

	bool is_classification = isClassification();

	interpolation = InterpolationConverter.from_json(json, "interpolation");
	if (is_classification)
		interpolation = Interpolation::Discrete;

	if (!is_classification && json.isMember("classes"))
		throw ArgumentException("Unit string invalid: Found a class, but the unit is not a classification");

	if (is_classification) {
		auto c = json.get("classes", Json::Value(Json::ValueType::objectValue));
		if (!c.isObject() || c.empty())
			throw ArgumentException("Unit string invalid: Classes must be specificed as a non-empty object");

	    for (auto itr = c.begin() ; itr != c.end() ; itr++) {
	        auto key = itr.key().asString();
	        bool is_integer_key = false;
	        int key_int = 0;
			try {
				size_t idx = 0;
				key_int = std::stoi(key, &idx);
				if (idx == key.size())
					is_integer_key = true;
			}
			catch (...) {
			}
			if (!is_integer_key)
				throw ArgumentException("Unit string invalid: a class specification must have an integer key");

			classes.insert( std::make_pair((uint32_t) key_int, Class{(*itr).asString()}) );
	    }

		if (classes.size() == 0)
			throw ArgumentException("Unit string invalid: Cannot use a classification without specifying any classes");
	}
}


Unit::~Unit() {

}

std::string Unit::toJson() const {
	Json::Value root(Json::ValueType::objectValue);
	root["measurement"] = measurement;
	root["unit"] = unit;
	if (std::isfinite(min))
		root["min"] = min;
	if (std::isfinite(max))
		root["max"] = max;
	root["interpolation"] = InterpolationConverter.to_string(interpolation);

	if (isClassification()) {
		Json::Value classes(Json::ValueType::objectValue);
		for (auto p : this->classes)
			classes[std::to_string(p.first)] = p.second.getName();
		root["classes"] = classes;
	}

	Json::FastWriter writer;
	return writer.write(root);
}
