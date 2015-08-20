#include "timemodification.h"

#include "util/exceptions.h"

#include "util/make_unique.h"

auto TimeShift::toPTime(time_t time) -> PTime {
	return boost::posix_time::from_time_t(time);
}

auto TimeShift::toTime_t(PTime pTime) -> time_t {
	// should be at some point in the library
	// https://github.com/boostorg/date_time/blob/master/include/boost/date_time/posix_time/conversion.hpp
	boost::posix_time::time_duration dur = pTime - boost::posix_time::ptime(boost::gregorian::date(1970,1,1));
	return std::time_t(dur.total_seconds());
}

auto Identity::apply(const time_t& input) -> time_t {
	return input;
}

auto Identity::reverse(const time_t& input) -> time_t {
	return input;
}

const std::map<std::string, RelativeShift::ShiftUnit> RelativeShift::string_to_enum{
	{"seconds", ShiftUnit::seconds},
	{"minutes", ShiftUnit::minutes},
	{"hours", ShiftUnit::hours},
	{"days", ShiftUnit::days},
	{"months", ShiftUnit::months},
	{"years", ShiftUnit::years}
};

auto RelativeShift::createUnit(std::string value) -> ShiftUnit {
	return string_to_enum.at(value);
}

auto RelativeShift::apply(const time_t& input) -> time_t {
	PTime ptime = shift(toPTime(input));
	time_t result = toTime_t(ptime);
	time_difference = result - input;
	return result;
}

auto RelativeShift::reverse(const time_t& input) -> time_t {
	return input - time_difference;
}

auto RelativeShift::shift(const PTime time) const -> PTime {
	switch (unit) {
		case ShiftUnit::seconds:
			return PTime(time.date(), time.time_of_day() + boost::posix_time::seconds(shift_value));
		case ShiftUnit::minutes:
			return PTime(time.date(), time.time_of_day() + boost::posix_time::minutes(shift_value));
		case ShiftUnit::hours:
			return PTime(time.date(), time.time_of_day() + boost::posix_time::hours(shift_value));

		case ShiftUnit::days:
			return PTime(time.date() + boost::gregorian::days(shift_value), time.time_of_day());
		case ShiftUnit::months:
			return PTime(time.date() + boost::gregorian::months(shift_value), time.time_of_day());
		case ShiftUnit::years:
			return PTime(time.date() + boost::gregorian::years(shift_value), time.time_of_day());
	}
}

auto AbsoluteShift::apply(const time_t& input) -> time_t {
	time_difference = result_time - input;
	return result_time;
}

auto AbsoluteShift::reverse(const time_t& input) -> time_t {
	return input - time_difference;
}

auto Stretch::apply(const time_t& input) -> time_t {
	PTime time = toPTime(input);

	auto duration = time - fixedPoint;
	auto result_time = toTime_t(time + (duration * factor));

	time_difference = result_time - input;
	return result_time;
}

auto Stretch::reverse(const time_t& input) -> time_t {
	return input - time_difference;
}

auto Snap::apply(const time_t& input) -> time_t {
	PTime time = toPTime(input);

	time_t result_time;

	switch (unit) {
		case SnapUnit::dayInMonth:
			{
				Date date = time.date();
				Date snappedDate;
				try {
					snappedDate = Date{date.year(), date.month(), value};
				} catch (boost::gregorian::bad_day_of_month& e) {
					if(value > 28) { // exceeded end of month
						snappedDate = date.end_of_month();
					} else {
						throw e;
					}
				}


				if(allow_reset) {
					result_time = toTime_t(PTime(snappedDate, boost::posix_time::seconds(0)));
				} else {
					result_time = toTime_t(PTime(snappedDate, time.time_of_day()));
				}
			}
			break;
		case SnapUnit::dayInYear:
			{
				Date date = time.date();
				Date shiftedDate = date + DateDuration{value - date.day_of_year()};

				if(allow_reset) {
					result_time = toTime_t(PTime(shiftedDate, boost::posix_time::seconds(0)));
				} else {
					result_time = toTime_t(PTime(shiftedDate, time.time_of_day()));
				}
			}
			break;
		case SnapUnit::seasonInYear:
			{
				Date date = time.date();
				short unsigned int month;

				switch (value) {
					case 1:
						month = boost::date_time::Jan;
						break;
					case 2:
						month = boost::date_time::Apr;
						break;
					case 3:
						month = boost::date_time::Jul;
						break;
					case 4:
						month = boost::date_time::Oct;
						break;
				}

				if(allow_reset) {
					result_time = toTime_t(PTime{Date{date.year(), month, 1}, boost::posix_time::seconds{0}});
				} else {
					result_time = toTime_t(PTime(Date{date.year(), month, date.day()}, time.time_of_day()));
				}
			}
			break;
		case SnapUnit::dayInWeek:
			{
				int day_of_week = time.date().day_of_week();
				day_of_week = day_of_week == 0 ? 7 : day_of_week; // adjust sunday

				std::unique_ptr<boost::gregorian::greg_weekday> weekday;
				switch (value) {
					case 1:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Monday);
						break;
					case 2:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Tuesday);
						break;
					case 3:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Wednesday);
						break;
					case 4:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Thursday);
						break;
					case 5:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Friday);
						break;
					case 6:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Saturday);
						break;
					case 7:
						weekday = make_unique<boost::gregorian::greg_weekday>(boost::date_time::Sunday);
						break;
				}

				Date snappedDate;
				if(value < day_of_week) {
					snappedDate = boost::date_time::previous_weekday(time.date(), *weekday);
				} else {
					snappedDate = boost::date_time::next_weekday(time.date(), *weekday);
				}

				if(allow_reset) {
					result_time = toTime_t(PTime(snappedDate, boost::posix_time::seconds(0)));
				} else {
					result_time = toTime_t(PTime(snappedDate, time.time_of_day()));
				}
			}
			break;
		case SnapUnit::monthInYear:
			{
				Date date = time.date();

				if(allow_reset) {
					result_time = toTime_t(PTime{Date{date.year(), value, 1}, boost::posix_time::seconds{0}});
				} else {
					result_time = toTime_t(PTime{Date{date.year(), value, date.day()}, time.time_of_day()});
				}
			}
			break;
		case SnapUnit::hourOfDay:
			{
				auto timeOfDay = time.time_of_day();

				if(allow_reset) {
					result_time = toTime_t(PTime{time.date(), boost::posix_time::hours(value)});
				} else {
					result_time = toTime_t(PTime{time.date(), TimeDuration{value, timeOfDay.minutes(), timeOfDay.seconds()}});
				}
			}
			break;
	}

	time_difference = result_time - input;
	return result_time;
}

auto Snap::reverse(const time_t& input) -> time_t {
	return input - time_difference;
}

const std::map<std::string, Snap::SnapUnit> Snap::string_to_enum {
	{"dayInMonth", SnapUnit::dayInMonth},
	{"dayInYear", SnapUnit::dayInYear},
	{"seasonInYear", SnapUnit::seasonInYear},
	{"dayInWeek", SnapUnit::dayInWeek},
	{"monthInYear", SnapUnit::monthInYear},
	{"hourOfDay", SnapUnit::hourOfDay}
};

auto Snap::createUnit(std::string value) -> SnapUnit {
	return string_to_enum.at(value);
}

auto TimeModification::apply(const TemporalReference& input) -> const TemporalReference {
	isApplyCalled = true;

	switch(input.timetype) {
		case TIMETYPE_UNKNOWN:
			throw OperatorException("It is not possible to modify an unknown time type.");
		case TIMETYPE_UNREFERENCED:
			throw OperatorException("It is not possible to modify an unreferenced time type.");
		case TIMETYPE_UNIX:
			std::time_t time_from = static_cast<std::time_t>(input.t1);
			std::time_t time_to = static_cast<std::time_t>(input.t2);

			time_from = from_shift->apply(time_from);
			time_to = to_shift->apply(time_to);

			time_from = stretch->apply(time_from);
			time_to = stretch->apply(time_to);

			time_from = from_snap->apply(time_from);
			time_to = to_snap->apply(time_to);

			return TemporalReference{TIMETYPE_UNIX, static_cast<double>(time_from), static_cast<double>(time_to)};
	}
}

/**
 * Revert the time shift for the output timestamp.
 *
 * @param input
 *
 * @return reverted output time.
 */
auto TimeModification::reverse(const TemporalReference& input) -> const TemporalReference {
	if(!isApplyCalled) {
		throw OperatorException("You must call apply before reverse.");
	}

	switch(input.timetype) {
		case TIMETYPE_UNKNOWN:
			throw OperatorException("It is not possible to modify an unknown time type.");
		case TIMETYPE_UNREFERENCED:
			throw OperatorException("It is not possible to modify an unreferenced time type.");
		case TIMETYPE_UNIX:
			std::time_t time_from = static_cast<std::time_t>(input.t1);
			std::time_t time_to = static_cast<std::time_t>(input.t2);

			time_from = from_snap->reverse(time_from);
			time_to = to_snap->reverse(time_to);

			time_from = stretch->reverse(time_from);
			time_to = stretch->reverse(time_to);

			time_from = from_shift->reverse(time_from);
			time_to = to_shift->reverse(time_to);

			return TemporalReference{TIMETYPE_UNIX, static_cast<double>(time_from), static_cast<double>(time_to)};
	}
}
