#include <gtest/gtest.h>

#include "util/timeparser.h"
#include "util/exceptions.h"


TEST(TimeParser, testSeconds){
	auto parser = TimeParser::create(TimeParser::Format::SECONDS);

	EXPECT_FLOAT_EQ(1447240271, parser->parse("1447240271"));

	EXPECT_THROW(parser->parse("TEXT"), TimeParseException);
}

TEST(TimeParser, testDMHYM){
	auto parser = TimeParser::create(TimeParser::Format::DMYHM);

	EXPECT_FLOAT_EQ(1447240260, parser->parse("11-Nov-2015  11:11"));

	EXPECT_THROW(parser->parse("32-Nov-2015  11:11"), TimeParseException);
}

TEST(TimeParser, testISO){
	auto parser = TimeParser::create(TimeParser::Format::ISO);

	EXPECT_FLOAT_EQ(1447240271, parser->parse("2015-11-11T11:11:11"));

	EXPECT_THROW(parser->parse("2015-11-32T11:11:11"), TimeParseException);

	TemporalReference tref(timetype_t::TIMETYPE_UNIX);
	EXPECT_FLOAT_EQ(tref.beginning_of_time(), parser->parse("-infinity"));
	EXPECT_FLOAT_EQ(tref.end_of_time(), parser->parse("infinity"));
}

TEST(TimeParser, testCustom){
	auto parser = TimeParser::createCustom("%d.%m.%y %H:%M");

	EXPECT_FLOAT_EQ(1447240260, parser->parse("11.11.15 11:11"));

	EXPECT_THROW(parser->parse("32.11.15 11:11"), TimeParseException);
}
