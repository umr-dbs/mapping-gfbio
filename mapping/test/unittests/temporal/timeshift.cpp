#include <gtest/gtest.h>

#include "util/timemodification.h"
#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"

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

TEST_F(TimeShiftTests, Stretch) {
	Stretch stretch{boost::posix_time::time_from_string("2015-01-01 00:00:00"), 2};

	std::time_t stretched_time = stretch.apply(1427846400); // 2015-04-01 00:00:00

	// + 2 * (31+28+31 = 90) days

	ASSERT_EQ(1435622400, stretched_time); // 2015-06-30 00:00:00
}

TEST_F(TimeShiftTests, StretchWithFractions) {
	Stretch stretch{boost::posix_time::time_from_string("2015-01-01 00:00:00"), 2};

	double stretched_time = stretch.apply(1420416000.25); // 2015-01-05 00:00:00.25

	// + 2 * (4 days + 0.25 seconds)

	ASSERT_DOUBLE_EQ(1420761600.5, stretched_time); // 2015-01-09 00:00:00.5
}

TEST_F(TimeShiftTests, Snap_DayInMonth) {
	Snap snap{Snap::SnapUnit::dayInMonth, 5, true};
	std::time_t snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1441411200, snapped_time); // 2015-09-05 00:00:00

	snap = Snap{Snap::SnapUnit::dayInMonth, 5, false};
	snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1441411201, snapped_time); // 2015-09-05 00:00:01

	snap = Snap{Snap::SnapUnit::dayInMonth, 31, true};
	snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1443571200, snapped_time); // 2015-09-30 00:00:00
}

TEST_F(TimeShiftTests, Snap_DayInMonthWithFractions) {
	Snap snap{Snap::SnapUnit::dayInMonth, 5, true};
	double snapped_time = snap.apply(1443398401.625); // 2015-09-28 00:00:01

	EXPECT_DOUBLE_EQ(1441411200, snapped_time); // 2015-09-05 00:00:00

	snap = Snap{Snap::SnapUnit::dayInMonth, 5, false};
	snapped_time = snap.apply(1443398401.65); // 2015-09-28 00:00:01

	EXPECT_EQ(1441411201.65, snapped_time); // 2015-09-05 00:00:01

	snap = Snap{Snap::SnapUnit::dayInMonth, 31, true};
	snapped_time = snap.apply(1443398401.345); // 2015-09-28 00:00:01

	EXPECT_EQ(1443571200, snapped_time); // 2015-09-30 00:00:00
}

TEST_F(TimeShiftTests, Snap_DayInYear) {
	Snap snap{Snap::SnapUnit::dayInYear, 20, true};
	std::time_t snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1421712000, snapped_time); // 2015-01-20 00:00:00

	snap = Snap{Snap::SnapUnit::dayInYear, 20, false};
	snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1421712001, snapped_time); // 2015-01-20 00:00:01
}

TEST_F(TimeShiftTests, Snap_SeasonInYear) {
	Snap snap{Snap::SnapUnit::seasonInYear, 1, true};
	std::time_t snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1420070400, snapped_time); // 2015-01-01 00:00:00

	snap = Snap{Snap::SnapUnit::seasonInYear, 1, false};
	snapped_time = snap.apply(1443398401); // 2015-09-28 00:00:01

	EXPECT_EQ(1422403201, snapped_time); // 2015-01-28 00:00:01
}

TEST_F(TimeShiftTests, Snap_DayInWeek) {
	Snap snap{Snap::SnapUnit::dayInWeek, 1, true};
	std::time_t snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1443398400, snapped_time); // 2015-09-28 00:00:00

	snap = Snap{Snap::SnapUnit::dayInWeek, 5, false};
	snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1443744001, snapped_time); // 2015-10-02 00:00:01
}

TEST_F(TimeShiftTests, Snap_MonthInYear) {
	Snap snap{Snap::SnapUnit::monthInYear, 4, true};
	std::time_t snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1427846400, snapped_time); // 2015-04-01 00:00:00

	snap = Snap{Snap::SnapUnit::monthInYear, 4, false};
	snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1430352001, snapped_time); // 2015-04-30 00:00:01
}

TEST_F(TimeShiftTests, Snap_HourOfDay) {
	Snap snap{Snap::SnapUnit::hourOfDay, 10, true};
	std::time_t snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1443607200, snapped_time); // 2015-09-30 10:00:00

	snap = Snap{Snap::SnapUnit::hourOfDay, 10, false};
	snapped_time = snap.apply(1443571201); // 2015-09-30 00:00:01

	EXPECT_EQ(1443607201, snapped_time); // 2015-09-30 10:00:01
}

TEST_F(TimeShiftTests, TimeModification) {
	auto shift1 = make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2), make_unique<Identity>(), make_unique<Identity>(), make_unique<Identity>()};

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

TEST_F(TimeShiftTests, TimeModificationCheckReverse) {
	auto shift1 = make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2), make_unique<Identity>(), make_unique<Identity>(), make_unique<Identity>()};

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
	auto shift1 = make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2), make_unique<Identity>(), make_unique<Identity>(), make_unique<Identity>()};

	TemporalReference temporal_reference{timetype_t::TIMETYPE_UNIX, static_cast<double>(start_time), static_cast<double>(start_time + 1)};

	ASSERT_THROW(time_modification.reverse(temporal_reference), OperatorException);
}

TEST_F(TimeShiftTests, TimeModificationShiftWithFractions) {
	auto shift1 = make_unique<RelativeShift>(-5, RelativeShift::ShiftUnit::days);
	auto shift2 = make_unique<RelativeShift>(5, RelativeShift::ShiftUnit::minutes);
	TimeModification time_modification{std::move(shift1), std::move(shift2), make_unique<Identity>(), make_unique<Identity>(), make_unique<Identity>()};

	TemporalReference temporal_reference{timetype_t::TIMETYPE_UNIX, static_cast<double>(start_time + 0.25), static_cast<double>(start_time + 1.25)};
	auto shifted = time_modification.apply(temporal_reference);

	double time1 = 1419638400.25; // 2014-12-27 00:00:00.25
	ASSERT_DOUBLE_EQ(shifted.t1, time1);

	double time2 = 1420070701.25; // 2015-01-01 00:05:01.25
	ASSERT_DOUBLE_EQ(shifted.t2, time2);

	auto reversed = time_modification.reverse(shifted);

	ASSERT_DOUBLE_EQ(reversed.t1, start_time + 0.25);
	ASSERT_DOUBLE_EQ(reversed.t2, start_time + 1.25);
}
