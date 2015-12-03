
#include "timeparser.h"
#include "util/make_unique.h"
#include "util/exceptions.h"
#include "util/enumconverter.h"


const std::vector< std::pair<TimeParser::Format, std::string> > timeFormatMap = {
	std::make_pair(TimeParser::Format::SECONDS, "seconds"),
	std::make_pair(TimeParser::Format::DMYHM, "dmyhm"),
	std::make_pair(TimeParser::Format::ISO, "iso"),
	std::make_pair(TimeParser::Format::CUSTOM, "custom")
};

EnumConverter<TimeParser::Format> timeFormatConverter(timeFormatMap);

TimeParser::TimeParser(timetype_t timeType, Format format) : timeType(timeType), format(format){
}

Json::Value TimeParser::toJsonObject() const{
	Json::Value root(Json::ValueType::objectValue);
	root["format"] = timeFormatConverter.to_string(format);
	return root;
}

std::string TimeParser::toJson() const{
	Json::FastWriter writer;

	return writer.write(toJsonObject());
}




/**
 * Parser for UNIX timestamps in seconds
 */
class TimeParserSeconds : public TimeParser {
public:
	TimeParserSeconds() : TimeParser(timetype_t::TIMETYPE_UNIX, Format::SECONDS){}

	virtual double parse(const std::string& timeString) const{
		try {
			return stod(timeString);
		} catch (const std::invalid_argument& e){
			throw TimeParseException(concat("Could not parse timeString ", timeString, " : invalid argument"));
		} catch (const std::out_of_range& e){
			throw TimeParseException(concat("Could not parse timeString ", timeString, " : out of range"));
		}
	}
};

/**
 * Parser for time as "%d-%B-%Y  %H:%M"
 */
class TimeParserDMYHM : public TimeParser {
public:
	TimeParserDMYHM() : TimeParser(timetype_t::TIMETYPE_UNIX, Format::DMYHM){}

	virtual double parse(const std::string& timeString) const{
		std::tm tm = {};
		if (strptime(timeString.c_str(), "%d-%B-%Y  %H:%M", &tm))
			return timegm(&tm);
		throw TimeParseException(concat("Could not parse time string ", timeString, " for DMYHM format"));
	}
};

/**
 * Parser for Strings in ISO8601 format
 */
class TimeParserISO : public TimeParser {
public:

	TimeParserISO() : TimeParser(timetype_t::TIMETYPE_UNIX, Format::ISO){}

	virtual double parse(const std::string& timeString) const{
		//TODO: support entirety of ISO8601 compatible formats https://en.wikipedia.org/wiki/ISO_8601
		std::tm tm = {};
		if (strptime(timeString.c_str(), "%Y-%m-%dT%H:%M:%S", &tm))
			return timegm(&tm);
		throw TimeParseException(concat("Could not parse time string ", timeString, " for ISO format"));
	}
};

/**
 * Parser for time in a custom format that is strptime compatible
 */
class TimeParserCustom : public TimeParser {
public:

	TimeParserCustom(const std::string& custom_format) : TimeParser(timetype_t::TIMETYPE_UNIX, Format::CUSTOM), custom_format(custom_format){}

	virtual double parse(const std::string& timeString) const{
		std::tm tm = {};
		if (strptime(timeString.c_str(), custom_format.c_str(), &tm))
			return timegm(&tm);
		throw TimeParseException(concat("Could not parse time string: ", timeString, " for format: ", custom_format));
	}

	virtual Json::Value toJsonObject() const {
		Json::Value root(Json::ValueType::objectValue);
		root["format"] = timeFormatConverter.to_string(format);
		root["custom_format"] = custom_format;
		return root;
	}

private:
	std::string custom_format;

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

std::unique_ptr<TimeParser> TimeParser::createFromJson(const Json::Value& json) {
	Format format = timeFormatConverter.from_json(json, "format");

	if(format == Format::CUSTOM){
		if(!json.isMember("custom_format"))
			throw ArgumentException("TimeFormat is custom, but no custom format defined.");

		return make_unique<TimeParserCustom>(json.get("custom_format", "").asString());
	} else {
		return create(format);
	}
}
