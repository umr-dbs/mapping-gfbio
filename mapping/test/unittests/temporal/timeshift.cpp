#include <gtest/gtest.h>

#include "util/timemodification.h"
#include "datatypes/spatiotemporal.h"
#include "raster/exceptions.h"

#include <ctime>
#include <memory>
#include "util/make_unique.h"

class TimeShiftTests: public ::testing::Test {
protected:
	/**
	 * 2015-01-01 00:00:00
	 */
	std::time_t start_time = 1420070400;
};

TEST_F(TimeShiftTests, IdentitiyShift) {
	Identity shift;

	std::time_t shifted_time = shift.apply(start_time);
	ASSERT_EQ(shifted_time, start_time);

	ASSERT_EQ(shift.reverse(shifted_time), start_time);
}

TEST_F(TimeShiftTests, RelativeShift) {
	RelativeShift shift{5, RelativeShift::ShiftUnit::days};

	/**
	 * 2015-01-06 00:00:00
	 */
	std::time_t end_time = 1420502400;

	std::time_t shifted_time = shift.apply(start_time);
	ASSERT_EQ(shifted_time, end_time);

	ASSERT_EQ(shift.reverse(shifted_time), start_time);
}

TEST_F(TimeShiftTests, AbsoluteShift) {
	AbsoluteShift shift{boost::posix_time::time_from_string("2015-01-06 00:00:00")};

	/**
	 * 2015-01-06 00:00:00
	 */
	std::time_t end_time = 1420502400;

	std::time_t shifted_time = shift.apply(start_time);
	ASSERT_EQ(shifted_time, end_time);

	ASSERT_EQ(shift.reverse(shifted_time), start_time);
}

TEST_F(TimeShiftTests, TimeModification) {
	auto shift1 = std::make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = std::make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2)};

	TemporalReference temporal_reference{timetype_t::TIMETYPE_UNIX, static_cast<double>(start_time), static_cast<double>(start_time + 1)};
	auto shifted = time_modification.apply(temporal_reference);

	std::time_t time1 = 1419638400; // 2014-12-27 00:00:00
	ASSERT_EQ(static_cast<time_t>(shifted.t1), time1);

	std::time_t time2 = 1420070701; // 2015-01-01 00:05:01
	ASSERT_EQ(static_cast<time_t>(shifted.t2), time2);

	auto reversed = time_modification.reverse(shifted);

	ASSERT_EQ(static_cast<time_t>(reversed.t1), start_time);
	ASSERT_EQ(static_cast<time_t>(reversed.t2), start_time + 1);
}

TEST_F(TimeShiftTests, TimeModificationReverseBeforeShift) {
	auto shift1 = std::make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = std::make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2)};

	TemporalReference temporal_reference{timetype_t::TIMETYPE_UNIX, static_cast<double>(start_time), static_cast<double>(start_time + 1)};

	ASSERT_THROW(time_modification.reverse(temporal_reference), OperatorException);
}
