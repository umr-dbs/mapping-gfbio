#ifndef DATATYPES_UNIT_H
#define DATATYPES_UNIT_H

#include <string>
#include <map>
#include <vector>

/*
 * A Unit contains semantical information about a set of values (i.e. a Raster's pixels or an Attribute)
 * These are:
 *
 * - What is measured? e.g. Temperature, Elevation, Precipitation, ...
 * - What unit is the measurement in? e.g. Celsius, Kelvin, Meters, cm/day, ...
 * - Does it have a minimum or maximum value?
 * - is it a continuous or a discrete value (e.g. temperature vs. classification)?
 * - an optional set of parameters, e.g. names for a classification's classes
 *
 * Units can suggest a default colorization.
 */


namespace Json {
	class Value;
}


class Unit {
	public:
		enum class Interpolation {
			Unknown,
			Continuous,
			Discrete
		};
	protected:
		class Class {
			public:
				Class(const std::string &name) : name(name) {}
				const std::string &getName() const { return name; }
			private:
				std::string name;
				// TODO: color?
		};
	public:
		Unit(const std::string &description);
		Unit(const Json::Value &json);
		~Unit();
		std::string toJson() const;

		bool isContinuous() const { return interpolation == Interpolation::Continuous; }
		bool isDiscrete() const { return interpolation == Interpolation::Discrete; }
		bool isClassification() const { return unit == "classification"; }
		double getMin() const { return min; }
		double getMax() const { return max; }

		const std::string &getMeasurement() const { return measurement; }
		const std::string &getUnit() const { return unit; }
		const Class &getClass(size_t idx) const { return classes.at(idx); }
		//const std::string &getParam(const std::string &key) { return params.at(key); }

	private:
		void init(const Json::Value &json);

		std::string measurement;
		std::string unit;
		Interpolation interpolation;
		std::map<int32_t, Class> classes;
		//std::map<std::string, std::string> params;
		double min, max;
		// Classification colors?
		// accuracy?
		// freeform parameters?
};





#endif
