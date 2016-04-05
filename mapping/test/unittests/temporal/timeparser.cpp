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
	EXPECT_FLOAT_EQ(tref.beginning_of_time(), parser->parse("0001-01-01T00:00:00"));
	EXPECT_FLOAT_EQ(tref.end_of_time(), parser->parse("9999-12-31T23:59:59"));
}

TEST(TimeParser, testCustom){
	auto parser = TimeParser::createCustom("%d.%m.%y %H:%M");

	EXPECT_FLOAT_EQ(1447240260, parser->parse("11.11.15 11:11"));

	EXPECT_THROW(parser->parse("32.11.15 11:11"), TimeParseException);
}

TEST(TimeParser, testISOBefore1970){
	auto parser = TimeParser::create(TimeParser::Format::ISO);

	EXPECT_FLOAT_EQ(-7783735800, parser->parse("1723-05-06T11:11:11"));
}

TEST(TimeParser, bot) {
	auto timeParser = TimeParser::create(TimeParser::Format::ISO);
	auto bot = timeParser->parse("0001-01-01T00:00:00");

	TemporalReference t(TIMETYPE_UNIX);
	EXPECT_FLOAT_EQ(bot, t.beginning_of_time());
}

TEST(TimeParser, eot) {
	auto timeParser = TimeParser::create(TimeParser::Format::ISO);
	auto eot = timeParser->parse("9999-12-31T23:59:59");

	TemporalReference t(TIMETYPE_UNIX);
	EXPECT_FLOAT_EQ(eot, t.end_of_time());
}
