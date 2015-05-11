#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <ctime>
#include <json/json.h>
#include <map>

#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#include "datatypes/raster.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/spatiotemporal.h"

#include "raster/exceptions.h"

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
		static auto toPTime(time_t time) -> PTime {
			return boost::posix_time::from_time_t(time);
		}

		/**
		 * Internal conversion function from ptime to time_t.
		 */
		static auto toTime_t(PTime pTime) -> time_t {
			// should be at some point in the library
			// https://github.com/boostorg/date_time/blob/master/include/boost/date_time/posix_time/conversion.hpp
			boost::posix_time::time_duration dur = pTime - boost::posix_time::ptime(boost::gregorian::date(1970,1,1));
			return std::time_t(dur.total_seconds());
		}
};

/**
 * This operator yields the identity of the input.
 */
class Identity : public TimeShift {
	public:
		auto apply(const time_t& input) -> time_t {
			return input;
		}
		auto reverse(const time_t& input) -> time_t {
			return input;
		}
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
		static auto createUnit(std::string value) -> ShiftUnit {
			return string_to_enum.at(value);
		}

		/**
		 * Creates a relative shift instance.
		 *
		 * @param amount
		 * @param unit
		 */
		RelativeShift(int amount, ShiftUnit unit) : unit(unit), shift_value(amount) {}

		auto apply(const time_t& input) -> time_t {
			PTime ptime = shift(toPTime(input));
			time_t result = toTime_t(ptime);
			time_difference = result - input;
			return result;
		}

		auto reverse(const time_t& input) -> time_t {
			return input - time_difference;
		}

	private:
		ShiftUnit unit;
		int shift_value;

		static const std::map<std::string, ShiftUnit> string_to_enum;

		auto shift(const PTime time) const -> PTime {
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

		time_t time_difference = 0;
};
const std::map<std::string, RelativeShift::ShiftUnit> RelativeShift::string_to_enum{
	{"seconds", ShiftUnit::seconds},
	{"minutes", ShiftUnit::minutes},
	{"hours", ShiftUnit::hours},
	{"days", ShiftUnit::days},
	{"months", ShiftUnit::months},
	{"years", ShiftUnit::years}
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

		auto apply(const time_t& input) -> time_t {
			time_difference = result_time - input;
			return result_time;
		}

		auto reverse(const time_t& input) -> time_t {
			return input - time_difference;
		}
	private:
		time_t result_time;

		// TODO: need better default behavior
		time_t time_difference = 0;
};

/**
 * A common virtual class for time modifications.
 */
class TimeModification {
	public:
		TimeModification(std::unique_ptr<TimeShift> from_shift, std::unique_ptr<TimeShift> to_shift) : from_shift(std::move(from_shift)), to_shift(std::move(to_shift)) {}
		~TimeModification() = default;

		/**
		 * Shift the timestamp.
		 *
		 * @param input
		 *
		 * @return shifted time
		 */
		auto apply(const TemporalReference& input) -> const TemporalReference {
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
		auto reverse(const TemporalReference& input) -> const TemporalReference {
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
	private:
		std::unique_ptr<TimeShift> from_shift;
		std::unique_ptr<TimeShift> to_shift;

		bool isApplyCalled = false;
};

/**
 * Change the time of the query rectangle.
 */
class TimeShiftOperator : public GenericOperator {
	public:
		TimeShiftOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~TimeShiftOperator();

		virtual auto getRaster(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<GenericRaster>;
		virtual auto getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PointCollection>;
		virtual auto getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<LineCollection>;
		virtual auto getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PolygonCollection>;
	private:
		/**
		 * Create a time modification (shift) out of the JSON command.
		 *
		 * @param shift_parameter A {@ref Json} value containing shift information.
		 * @return A {@ref TimeModification} unique pointer.
		 */
		auto parseShiftValues(const Json::Value& shift_parameter) -> std::unique_ptr<TimeShift>;

		std::unique_ptr<TimeModification> time_modification;
};

auto TimeShiftOperator::parseShiftValues(const Json::Value& shift_parameter) -> std::unique_ptr<TimeShift> {
	std::string unit = shift_parameter.get("unit", "none").asString();
	if(unit.compare("none") == 0) {
		throw ArgumentException("Unit must not be <none>.");
	} else if(unit.compare("absolute") == 0) {
		std::string shift_value = shift_parameter.get("value", "").asString();
		// TODO: catch errors
		boost::posix_time::ptime time_value{boost::posix_time::time_from_string(shift_value)};
		return std::make_unique<AbsoluteShift>(time_value);
	} else {
		int shift_value = shift_parameter.get("value", "").asInt();
		RelativeShift::ShiftUnit unit_value = RelativeShift::createUnit(unit);
		return std::make_unique<RelativeShift>(shift_value, unit_value);
	}
}

TimeShiftOperator::TimeShiftOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	std::unique_ptr<TimeShift> from;
	std::unique_ptr<TimeShift> to;

	// process shift parameters
	auto shift_parameter = params.get("shift", Json::nullValue);
	if (!shift_parameter.isNull() && shift_parameter.isMember("from")) {
		from = parseShiftValues(shift_parameter.get("from", Json::nullValue));
	} else {
		from = std::make_unique<Identity>();
	}

	if (!shift_parameter.isNull() && shift_parameter.isMember("to")) {
		to = parseShiftValues(shift_parameter.get("to", Json::nullValue));
	} else {
		to = std::make_unique<Identity>();
	}

	// TODO: stretch

	// TODO: snap

	time_modification = std::make_unique<TimeModification>(std::move(from), std::move(to));
}

TimeShiftOperator::~TimeShiftOperator() {}
REGISTER_OPERATOR(TimeShiftOperator, "timeShiftOperator");

auto TimeShiftOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<GenericRaster> {
	// TODO: extract function

	TemporalReference temporal_reference(TIMETYPE_UNIX, rect.timestamp, rect.timestamp+1);

	TemporalReference modified_temporal_reference = time_modification->apply(temporal_reference);

	QueryRectangle query_rectangle{static_cast<time_t>(modified_temporal_reference.t1), rect.x1, rect.y1, rect.x2, rect.y2, rect.xres, rect.yres, rect.epsg};

	auto result = getRasterFromSource(0, query_rectangle, profiler);

	SpatioTemporalReference result_stref{result->stref, time_modification->reverse(result->stref)};
	result->replaceSTRef(result_stref);

	return result;
}

auto TimeShiftOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PointCollection> {
	TemporalReference temporal_reference(TIMETYPE_UNIX, rect.timestamp, rect.timestamp+1);

	TemporalReference modified_temporal_reference = time_modification->apply(temporal_reference);

	QueryRectangle query_rectangle{static_cast<time_t>(modified_temporal_reference.t1), rect.x1, rect.y1, rect.x2, rect.y2, rect.xres, rect.yres, rect.epsg};

	auto result = getPointCollectionFromSource(0, query_rectangle, profiler);

	SpatioTemporalReference result_stref{result->stref, time_modification->reverse(result->stref)};
	result->replaceSTRef(result_stref);

	return result;
}

auto TimeShiftOperator::getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<LineCollection> {
	TemporalReference temporal_reference(TIMETYPE_UNIX, rect.timestamp, rect.timestamp+1);

	TemporalReference modified_temporal_reference = time_modification->apply(temporal_reference);

	QueryRectangle query_rectangle{static_cast<time_t>(modified_temporal_reference.t1), rect.x1, rect.y1, rect.x2, rect.y2, rect.xres, rect.yres, rect.epsg};

	auto result = getLineCollectionFromSource(0, query_rectangle, profiler);

	SpatioTemporalReference result_stref{result->stref, time_modification->reverse(result->stref)};
	result->replaceSTRef(result_stref);

	return result;
}

auto TimeShiftOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PolygonCollection> {
	TemporalReference temporal_reference(TIMETYPE_UNIX, rect.timestamp, rect.timestamp+1);

	TemporalReference modified_temporal_reference = time_modification->apply(temporal_reference);

	QueryRectangle query_rectangle{static_cast<time_t>(modified_temporal_reference.t1), rect.x1, rect.y1, rect.x2, rect.y2, rect.xres, rect.yres, rect.epsg};

	auto result = getPolygonCollectionFromSource(0, query_rectangle, profiler);

	SpatioTemporalReference result_stref{result->stref, time_modification->reverse(result->stref)};
	result->replaceSTRef(result_stref);

	return result;
}
