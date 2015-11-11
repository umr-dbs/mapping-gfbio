#ifndef DATATYPES_UNIT_H
#define DATATYPES_UNIT_H

#include <string>
#include <map>
#include <vector>

namespace Json {
	class Value;
}


/**
 * @class Unit
 *
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
		class Uninitialized_t {
		};
	public:
		// No default constructor, because uninitialized Units are invalid and thus potentially dangerous
		Unit() = delete;
		/**
		 * Construct a unit without initializing any values. This is only useful if you absolutely must
		 * default construct a unit. For example, if a Unit is a member of your class, you can force it to
		 * remain uninitialized in the initializer list. Then you can properly initialize it in the constructor
		 * body.
		 *
		 * The only guaranteed way to turn an uninitialized unit into a valid unit is to copy-assign a valid unit.
		 *
		 * @param u pass the constant Unit::UNINITIALIZED
		 */
		Unit(Uninitialized_t u);
		static const Uninitialized_t UNINITIALIZED;
		/**
		 * Construct a unit from its JSON representation
		 *
		 * @param json the JSON representation as a string
		 */
		Unit(const std::string &json);
		/**
		 * Construct a unit from its JSON representation
		 *
		 * @param json the JSON representation as a Json::Value
		 */
		Unit(const Json::Value &json);
		/**
		 * Construct a unit containing just the minimum information to be valid.
		 *
		 * @param measurement The measurement.
		 * @param unit The unit.
		 */
		Unit(const std::string &measurement, const std::string &unit);
		~Unit();

		/**
		 * Verify if a Unit is considered valid. Throws an exception if it is not.
		 */
		void verify() const;

		/**
		 * A named constructor for an unknown unit.
		 *
		 * @returns A valid Unit with unknown measurement, unit and interpolation
		 */
		static Unit unknown();
		/**
		 * Returns the unit's JSON representation
		 * @returns the unit's JSON representation as a string
		 */
		std::string toJson() const;
		/**
		 * Returns the unit's JSON representation
		 * @returns the unit's JSON representation as a Json::Value
		 */
		Json::Value toJsonObject() const;

		/**
		 * Returns whether the interpolation is continuous or not.
		 */
		bool isContinuous() const { return interpolation == Interpolation::Continuous; }
		/**
		 * Returns whether the interpolation is discrete or not.
		 */
		bool isDiscrete() const { return interpolation == Interpolation::Discrete; }
		/**
		 * Overrides the interpolation
		 * @param i the new interpolation method
		 */
		void setInterpolation(Interpolation i) { interpolation = i; }
		/**
		 * Returns whether the unit is a classification
		 */
		bool isClassification() const { return unit == "classification"; }
		/**
		 * Returns the min value, defaults to -inf
		 */
		double getMin() const { return min; }
		/**
		 * Returns the max value, defaults to +inf
		 */
		double getMax() const { return max; }
		/**
		 * Returns true if both min and max are set and finite
		 */
		bool hasMinMax() const;
		/**
		 * Overrides the min and max values
		 */
		void setMinMax(double min, double max);
		/**
		 * returns the measurement (in lower case)
		 */
		const std::string &getMeasurement() const { return measurement; }
		/**
		 * returns the unit (in lower case)
		 */
		const std::string &getUnit() const { return unit; }

		//const Class &getClass(size_t idx) const { return classes.at(idx); }
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
