#include "datatypes/spatiotemporal.h"
#include "util/timeparser.h"

#include <string>
#include <gtest/gtest.h>
#include <ctime>

TEST(STRef, toISOBeginOfTime) {
	TemporalReference tref(timetype_t::TIMETYPE_UNIX);

	std::string iso = tref.toIsoString(tref.beginning_of_time());
	EXPECT_EQ("0001-01-01T00:00:00", iso);
}

TEST(STRef, toISOnEndOfTime) {
	TemporalReference tref(timetype_t::TIMETYPE_UNIX);

	std::string iso = tref.toIsoString(tref.end_of_time());
	EXPECT_EQ("9999-12-31T23:59:59", iso);
}

TEST(STRef, temporalIntersectionWithIntervalsToEndOfTime) {
	auto parser = TimeParser::create(TimeParser::Format::ISO);

	double time = parser->parse("2014-11-24T16:23:57");
	double timeRef= parser->parse("2015-01-01T00:00:00");

	TemporalReference tref(timetype_t::TIMETYPE_UNIX);

	tref.t1 = timeRef;
	tref.t2 = tref.end_of_time();

	EXPECT_TRUE(tref.intersects(time, tref.end_of_time()));
}
