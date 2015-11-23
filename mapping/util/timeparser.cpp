
#include "timeparser.h"
#include "util/make_unique.h"

std::unique_ptr<TimeParser> TimeParser::getTimeParser(const TimeFormat timeFormat) {
	switch (timeFormat.format){
	case TimeFormat::Format::SECONDS:
		return make_unique<TimeParserSeconds>(TimeParserSeconds());
	case TimeFormat::Format::DMYHM:
		return make_unique<TimeParserDMYHM>(TimeParserDMYHM());
	case TimeFormat::Format::ISO:
		return make_unique<TimeParserISO>(TimeParserISO());
	case TimeFormat::Format::CUSTOM:
		return make_unique<TimeParserCustom>(TimeParserCustom(timeFormat.custom_format));
	}
}
