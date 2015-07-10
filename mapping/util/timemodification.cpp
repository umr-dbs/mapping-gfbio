#include "timemodification.h"

#include "util/exceptions.h"

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

auto TimeModification::apply(const TemporalReference& input) -> const TemporalReference {
	isApplyCalled = true;

	switch(input.timetype) {
		case TIMETYPE_UNKNOWN:
			throw OperatorException("It is not possible to modify an unknown time type.");
		case TIMETYPE_UNREFERENCED:
			throw OperatorException("It is not possible to modify an unreferenced time type.");
		case TIMETYPE_UNIX:
			const std::time_t time_from = static_cast<std::time_t>(input.t1);
			const std::time_t time_to = static_cast<std::time_t>(input.t2);
			return TemporalReference(TIMETYPE_UNIX, static_cast<double>(from_shift->apply(time_from)), to_shift->apply(time_to));
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
			const std::time_t time_from = static_cast<std::time_t>(input.t1);
			const std::time_t time_to = static_cast<std::time_t>(input.t2);
			return TemporalReference(TIMETYPE_UNIX, static_cast<double>(from_shift->reverse(time_from)), to_shift->reverse(time_to));
	}
}
