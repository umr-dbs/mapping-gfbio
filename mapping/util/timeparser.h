#ifndef UTIL_TIMEPARSER_H_
#define UTIL_TIMEPARSER_H_

#include <ctime>
#include <json/json.h>
#include "util/make_unique.h"
#include "datatypes/spatiotemporal.h"


/**
 * This class provides methods to parse dates and datetimes from different
 * string representation
 */
class TimeParser {
public:

	enum class Format {
		SECONDS,  // unix timestamp as seconds
		DMYHM,   // %d-%B-%Y  %H:%M
		ISO,    // ISO 8601 format
		CUSTOM // custom format specified in field custom_format as strptime compatible string
	};

	/**
	 * Creates a parser for the given time format
	 * @param timeFormat the format for the parser
	 * @return a parser for the given time format
	 */
	static std::unique_ptr<TimeParser> create(const Format timeFormat);

	/**
	 * Creates a parser for a custom time format
	 * @param customFormat the strptime compatible format for the parser
	 * @return a parser for the given custom time format
	 */
	static std::unique_ptr<TimeParser> createCustom(const std::string& customFormat);

	/**
	 * Creates a parser from json (from operator params)
	 * @param json the json value that describes the time format
	 * @return a parser for the timeFormat given by the json value
	 */
	static std::unique_ptr<TimeParser> createFromJson(const Json::Value& json);

	/**
	 * Get the timetype for the resulting timestamps
	 * @return the timetype for the resulting timestamps
	 */
	timetype_t getTimeType(){
		return timeType;
	}

	/**
	 * get a json representation of the format for serialization
	 * @return json representation of the format for serialization
	 */
	virtual Json::Value toJsonObject() const;


	/**
	 * get a json representation as string of the format for serialization
	 * @return json representation as string of the format for serialization
	 */
	std::string toJson() const;

	/**
	 * parse the given time string and return a corresponding value
	 * @param timeString the string to parse
	 * @return the time extracted from the given string
	 */
	virtual double parse(const std::string& timeString) const = 0;

	virtual ~TimeParser() = default;

protected:
	TimeParser(const timetype_t timeType, Format format);

	timetype_t timeType;
	Format format;
};


#endif
