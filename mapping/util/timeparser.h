#ifndef UTIL_TIMEPARSER_H_
#define UTIL_TIMEPARSER_H_

#include "util/exceptions.h"
#include <ctime>
#include "util/make_unique.h"

/**
 * This class represents different TimeFormats and indicates the format
 * in which a date/time is represented as a String
 */
class TimeFormat {
public:
	enum class Format {
		SECONDS,  // unix timestamp as seconds
		DMYHM,   // %d-%B-%Y  %H:%M
		ISO,    // ISO 8601 format
		CUSTOM // custom format specified in field custom_format as strptime compatible string
	};

	/**
	 * get format for unix time in seconds
	 * @return format for unix time in seconds
	 */
	static TimeFormat getSecondsFormat(){
		return TimeFormat(Format::SECONDS);
	}

	/**
	 * get format for %d-%B-%Y  %H:%M
	 * @return format for %d-%B-%Y  %H:%M
	 */
	static TimeFormat getDMYHMFormat(){
		return TimeFormat(Format::DMYHM);
	}

	/**
	 * get format for ISO8601
	 * @return format for ISO 8601
	 */
	static TimeFormat getISOFormat(){
		return TimeFormat(Format::ISO);
	}

	/**
	 * get format for custom format
	 * @param custom_format strptime compatible custom format
	 * @return format for custom format
	 */
	static TimeFormat getCustomFormat(const std::string& custom_format){
		return TimeFormat(Format::CUSTOM, custom_format);
	}

	/**
	 * get format by name
	 * @param formatName the name of the format
	 * @return format for the given name
	 */
	static TimeFormat getFormatByName(const std::string& formatName){
		if(formatName == "seconds")
			return TimeFormat(Format::SECONDS);
		if(formatName == "dmyhm")
			return TimeFormat(Format::DMYHM);
		if(formatName =="iso")
			return TimeFormat(Format::ISO);

		throw ArgumentException("Invalid format for time");
	}

	Format format;
	std::string custom_format;

private:
	TimeFormat(Format format, std::string custom_format) : format(format), custom_format(custom_format){};

	TimeFormat(Format format) : format(format){};

};

/**
 * This class provides methods to parse dates and datetimes from different
 * string representation
 */
class TimeParser {
public:

	/**
	 * Creates a parser for the given time format
	 * @param timeFormat the format for the parser
	 * @return a parser for the given time format
	 */
	static std::unique_ptr<TimeParser> getTimeParser(const TimeFormat timeFormat);

	/**
	 * parse the given time string and return a corresponding value
	 * @param timeString the string to parse
	 * @return the time extracted from the given string
	 */
	virtual double parse(const std::string& timeString) const = 0;

	virtual ~TimeParser() = default;

protected:
	TimeParser() = default;

};

/**
 * Parser for UNIX timestamps in seconds
 */
class TimeParserSeconds : public TimeParser {
public:

	virtual double parse(const std::string& timeString) const{
		return stod(timeString);
	}
};

/**
 * Parser for time as "%d-%B-%Y  %H:%M"
 */
class TimeParserDMYHM : public TimeParser {
public:

	virtual double parse(const std::string& timeString) const{
		std::tm tm;
		if (strptime(timeString.c_str(), "%d-%B-%Y  %H:%M", &tm))
			return timegm(&tm);
		throw ConverterException("Could not parse time string");
	}
};

/**
 * Parser for Strings in ISO8601 format
 */
class TimeParserISO : public TimeParser {
public:

	virtual double parse(const std::string& timeString) const{
		//TODO: support entirety of ISO8601 combatible formats https://en.wikipedia.org/wiki/ISO_8601
		std::tm tm;
		if (strptime(timeString.c_str(), "%Y-%m-%dT%H:%M:%S", &tm))
			return timegm(&tm);
		throw ConverterException("Could not parse time string");
	}
};

/**
 * Parser for time in a custom format that is strptime compatible
 */
class TimeParserCustom : public TimeParser {
public:

	TimeParserCustom(const std::string format) : format(format){};

	virtual double parse(const std::string& timeString) const{
		std::tm tm;
		if (strptime(timeString.c_str(), format.c_str(), &tm))
			return timegm(&tm);
		throw ConverterException("Could not parse time string");
	}

private:
	std::string format;

};



#endif
