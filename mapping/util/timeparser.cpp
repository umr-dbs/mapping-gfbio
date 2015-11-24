
#include "timeparser.h"
#include "util/make_unique.h"
#include "util/exceptions.h"

TimeParser::TimeParser(timetype_t timeType) : timeType(timeType){
}

/**
 * Parser for UNIX timestamps in seconds
 */
class TimeParserSeconds : public TimeParser {
public:
	TimeParserSeconds() : TimeParser(timetype_t::TIMETYPE_UNIX){}

	virtual double parse(const std::string& timeString) const{
		try {
			return stod(timeString);
		} catch (const std::invalid_argument& e){
			throw TimeParseException("Could not parse timeString: invalid argument");
		} catch (const std::out_of_range& e){
			throw TimeParseException("Could not parse timeString: out of range");
		}
	}
};

/**
 * Parser for time as "%d-%B-%Y  %H:%M"
 */
class TimeParserDMYHM : public TimeParser {
public:
	TimeParserDMYHM() : TimeParser(timetype_t::TIMETYPE_UNIX){}

	virtual double parse(const std::string& timeString) const{
		std::tm tm;
		if (strptime(timeString.c_str(), "%d-%B-%Y  %H:%M", &tm))
			return timegm(&tm);
		throw TimeParseException("Could not parse time string");
	}
};

/**
 * Parser for Strings in ISO8601 format
 */
class TimeParserISO : public TimeParser {
public:

	TimeParserISO() : TimeParser(timetype_t::TIMETYPE_UNIX){}

	virtual double parse(const std::string& timeString) const{
		//TODO: support entirety of ISO8601 compatible formats https://en.wikipedia.org/wiki/ISO_8601
		std::tm tm;
		if (strptime(timeString.c_str(), "%Y-%m-%dT%H:%M:%S", &tm))
			return timegm(&tm);
		throw TimeParseException("Could not parse time string");
	}
};

/**
 * Parser for time in a custom format that is strptime compatible
 */
class TimeParserCustom : public TimeParser {
public:

	TimeParserCustom(const std::string format) : TimeParser(timetype_t::TIMETYPE_UNIX), format(format){}

	virtual double parse(const std::string& timeString) const{
		std::tm tm;
		if (strptime(timeString.c_str(), format.c_str(), &tm))
			return timegm(&tm);
		throw TimeParseException("Could not parse time string");
	}

private:
	std::string format;

};

std::unique_ptr<TimeParser> TimeParser::create(const Format timeFormat) {
	switch (timeFormat){
	case Format::SECONDS:
		return make_unique<TimeParserSeconds>();
	case Format::DMYHM:
		return make_unique<TimeParserDMYHM>();
	case Format::ISO:
		return make_unique<TimeParserISO>();
	}

	throw ArgumentException("Could not create TimeParser for given format");
}

std::unique_ptr<TimeParser> TimeParser::createCustom(const std::string& customFormat) {
	return make_unique<TimeParserCustom>(customFormat);
}
