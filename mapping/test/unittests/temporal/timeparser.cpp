#include <gtest/gtest.h>

#include "util/timeparser.h"


TEST(TimeParser, testSeconds){
	auto parser = TimeParser::getTimeParser(TimeFormat::getSecondsFormat());

	EXPECT_FLOAT_EQ(1447240271, parser->parse("1447240271"));
}

TEST(TimeParser, testDMHYM){
	auto parser = TimeParser::getTimeParser(TimeFormat::getDMYHMFormat());

	EXPECT_FLOAT_EQ(1447240260, parser->parse("11-Nov-2015  11:11"));
}

TEST(TimeParser, testISO){
	auto parser = TimeParser::getTimeParser(TimeFormat::getISOFormat());

	EXPECT_FLOAT_EQ(1447240271, parser->parse("2015-11-11T11:11:11"));
}

TEST(TimeParser, testCustom){
	auto parser = TimeParser::getTimeParser(TimeFormat::getCustomFormat("%d.%m.%y %H:%M"));

	EXPECT_FLOAT_EQ(1447240260, parser->parse("11.11.15 11:11"));
}
