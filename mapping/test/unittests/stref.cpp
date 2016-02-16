#include <datatypes/spatiotemporal.h>

#include <string>
#include <gtest/gtest.h>

TEST(STRef, toISOBeginEndOfTime) {
	TemporalReference tref(timetype_t::TIMETYPE_UNIX);

	std::string iso = tref.toIsoString(tref.beginning_of_time());
	EXPECT_EQ("-infinity", iso);

	iso = tref.toIsoString(tref.end_of_time());
	EXPECT_EQ("infinity", iso);
}
