#ifndef UTIL_TIMEPARSER_H_
#define UTIL_TIMEPARSER_H_

#include <ctime>
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
	 * Get the timetype for the resulting timestamps
	 * @return the timetype for the resulting timestamps
	 */
	timetype_t getTimeType(){
		return timeType;
	}

	/**
	 * parse the given time string and return a corresponding value
	 * @param timeString the string to parse
	 * @return the time extracted from the given string
	 */
	virtual double parse(const std::string& timeString) const = 0;

	virtual ~TimeParser() = default;

protected:
	TimeParser(const timetype_t timeType);

	timetype_t timeType;
};


#endif
