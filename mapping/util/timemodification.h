#pragma once

#include <map>
#include <memory>

#include <ctime>
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#include "datatypes/spatiotemporal.h"

class TimeShift {
	public:
		/**
		 * Shift the timestamp.
		 *
		 * @param input
		 *
		 * @return shifted time
		 */
		virtual auto apply(const time_t& input) -> time_t = 0;

		/**
		 * Revert the time shift for the output timestamp.
		 *
		 * @param input
		 *
		 * @return reverted output time.
		 */
		virtual auto reverse(const time_t& input) -> time_t = 0;

		virtual ~TimeShift() = default;
	protected:
		using time_t = std::time_t;

		using PTime = boost::posix_time::ptime;
		using Date = boost::gregorian::date;

		using TimeDuration = boost::posix_time::time_duration;
		using DateDuration = boost::gregorian::date_duration;

		/**
		 * Internal conversion function from time_t to ptime.
		 */
		static auto toPTime(time_t time) -> PTime;

		/**
		 * Internal conversion function from ptime to time_t.
		 */
		static auto toTime_t(PTime pTime) -> time_t;
};

/**
 * This operator yields the identity of the input.
 */
class Identity : public TimeShift {
	public:
		auto apply(const time_t& input) -> time_t;
		auto reverse(const time_t& input) -> time_t;
};

/**
 * Use a relative shift by amounts of units.
 */
class RelativeShift : public TimeShift {
	public:
		/**
		 * Units for time shifting.
		 */
		enum class ShiftUnit {
			seconds,
			minutes,
			hours,
			days,
			months,
			years
		};

		/**
		 * Creates a unit out of a string.
		 *
		 * @param value
		 */
		static auto createUnit(std::string value) -> ShiftUnit;

		/**
		 * Creates a relative shift instance.
		 *
		 * @param amount
		 * @param unit
		 */
		RelativeShift(int amount, ShiftUnit unit) : unit(unit), shift_value(amount) {}

		auto apply(const time_t& input) -> time_t;
		auto reverse(const time_t& input) -> time_t;

	private:
		ShiftUnit unit;
		int shift_value;

		static const std::map<std::string, ShiftUnit> string_to_enum;

		auto shift(const PTime time) const -> PTime;

		time_t time_difference = 0;
};

/**
 * Use an absolute posix timestamp to modify the time.
 */
class AbsoluteShift : public TimeShift {
	public:
		/**
		 * Creates an instance using a ptime.
		 *
		 * @param absolute_time
		 */
		AbsoluteShift(PTime absolute_time) : result_time(toTime_t(absolute_time)) {}

		auto apply(const time_t& input) -> time_t;
		auto reverse(const time_t& input) -> time_t;
	private:
		time_t result_time;

		time_t time_difference = 0;
};

/**
 * Stretch the time interval.
 */
class Stretch : public TimeShift {
	public:
		/**
		 * Create an instance using a fixed point and a factor.
		 *
		 * @param fixedPoint A starting point on which to determine the stretch interval.
		 * @param factor The stretching factor.
		 */
		Stretch(PTime fixedPoint, int factor) : fixedPoint(fixedPoint), factor(factor) {}

		auto apply(const time_t& input) -> time_t;
		auto reverse(const time_t& input) -> time_t;
	private:
		PTime fixedPoint;
		int factor;

		time_t time_difference = 0;
};

/**
 * Snap to a time variant.
 */
class Snap : public TimeShift {
	public:
		/**
		 * Units for time shifting.
		 */
		enum class SnapUnit {
		    dayInMonth,
		    dayInYear,
		    seasonInYear,
		    dayInWeek,
			monthInYear,
		    hourOfDay
		};

		/**
		 * Creates a unit out of a string.
		 *
		 * @param value
		 */
		static auto createUnit(std::string value) -> SnapUnit;

		/**
		 * Create an instance using a SnapUnit and a value.
		 *
		 * @param unit A definition of where to snap to.
		 * @param value The modification value for the snap unit.
		 */
		Snap(SnapUnit unit, short unsigned int value, bool allow_reset) : unit(unit), value(value), allow_reset(allow_reset) {}

		auto apply(const time_t& input) -> time_t;
		auto reverse(const time_t& input) -> time_t;
	private:
		SnapUnit unit;
		short unsigned int value;
		bool allow_reset;

		static const std::map<std::string, SnapUnit> string_to_enum;

		time_t time_difference = 0;
};

/**
 * A common virtual class for time modifications.
 */
class TimeModification {
	public:
		TimeModification(std::unique_ptr<TimeShift> from_shift,	std::unique_ptr<TimeShift> to_shift,
						std::unique_ptr<TimeShift> stretch,
						std::unique_ptr<TimeShift> from_snap,	std::unique_ptr<TimeShift> to_snap) :
				from_shift(std::move(from_shift)), to_shift(std::move(to_shift)),
				stretch(std::move(stretch)),
				from_snap(std::move(from_snap)), to_snap(std::move(to_snap)) {}

		TimeModification(TimeModification&& time_modification) :
			from_shift(std::move(time_modification.from_shift)), to_shift(std::move(time_modification.to_shift)),
			stretch(std::move(time_modification.stretch)),
			from_snap(std::move(time_modification.from_snap)), to_snap(std::move(time_modification.to_snap)),
			isApplyCalled(time_modification.isApplyCalled) {}

		~TimeModification() = default;

		/**
		 * Shift the timestamp.
		 *
		 * @param input
		 *
		 * @return shifted time
		 */
		auto apply(const TemporalReference& input) -> const TemporalReference;

		/**
		 * Revert the time shift for the output timestamp.
		 *
		 * @param input
		 *
		 * @return reverted output time.
		 */
		auto reverse(const TemporalReference& input) -> const TemporalReference;
	private:
		std::unique_ptr<TimeShift> from_shift;
		std::unique_ptr<TimeShift> to_shift;
		std::unique_ptr<TimeShift> stretch;
		std::unique_ptr<TimeShift> from_snap;
		std::unique_ptr<TimeShift> to_snap;

		bool isApplyCalled = false;
};
