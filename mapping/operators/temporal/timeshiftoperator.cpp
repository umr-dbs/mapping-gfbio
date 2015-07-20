#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <ctime>
#include <json/json.h>

#include "datatypes/raster.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/spatiotemporal.h"

#include "util/exceptions.h"

#include "util/timemodification.h"

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

		/**
		 * Shift a {@ref QueryRectangle} by using the time_modification.
		 *
		 * @param rect a query rectangle reference
		 * @return a shifted query rectagle
		 */
		inline auto shift(const QueryRectangle& rect) -> const QueryRectangle;

		/**
		 * Reverses the shift on a {@ref SpatioTemporalResult} by using the time_modification.
		 *
		 * @param result A spatio temporal result reference
		 */
		inline auto reverse(SpatioTemporalResult& result) -> void;

		/**
		 * Reverses the shift on the elements of a {@ref SimpleFeatureCollection} by using the time_modification.
		 *
		 * @param collection A simple feature collection
		 */
		inline auto reverseElements(SimpleFeatureCollection& collection) -> void;

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
		return make_unique<AbsoluteShift>(time_value);
	} else {
		int shift_value = shift_parameter.get("value", "").asInt();
		RelativeShift::ShiftUnit unit_value = RelativeShift::createUnit(unit);
		return make_unique<RelativeShift>(shift_value, unit_value);
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
		from = make_unique<Identity>();
	}

	if (!shift_parameter.isNull() && shift_parameter.isMember("to")) {
		to = parseShiftValues(shift_parameter.get("to", Json::nullValue));
	} else {
		to = make_unique<Identity>();
	}

	// TODO: stretch

	// TODO: snap

	time_modification = make_unique<TimeModification>(std::move(from), std::move(to));
}

TimeShiftOperator::~TimeShiftOperator() {}
REGISTER_OPERATOR(TimeShiftOperator, "timeShiftOperator");

inline auto TimeShiftOperator::shift(const QueryRectangle &rect) -> const QueryRectangle {
	TemporalReference temporal_reference { TIMETYPE_UNIX, static_cast<double>(rect.timestamp), static_cast<double>(rect.timestamp + 1) };

	TemporalReference modified_temporal_reference = time_modification->apply(temporal_reference);

	return QueryRectangle{static_cast<time_t>(modified_temporal_reference.t1), rect.x1, rect.y1, rect.x2, rect.y2, rect.xres, rect.yres, rect.epsg};
}

inline auto TimeShiftOperator::reverse(SpatioTemporalResult& result) -> void {
	SpatioTemporalReference result_stref{result.stref, time_modification->reverse(result.stref)};
	result.replaceSTRef(result_stref);
}

inline auto TimeShiftOperator::reverseElements(SimpleFeatureCollection& collection) -> void {
	if(collection.hasTime()) {
		for(size_t i = 0; i < collection.getFeatureCount(); ++i) {
			TemporalReference point_tref{TIMETYPE_UNIX, static_cast<double>(collection.time_start[i]), static_cast<double>(collection.time_end[i])};
			TemporalReference result_point_tref = time_modification->reverse(point_tref);
			collection.time_start[i] = static_cast<time_t>(result_point_tref.t1);
			collection.time_end[i] = static_cast<time_t>(result_point_tref.t2);
		}
	}
}

auto TimeShiftOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<GenericRaster> {
	auto result = getRasterFromSource(0, shift(rect), profiler);

	reverse(*result);

	return result;
}

auto TimeShiftOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PointCollection> {
	auto result = getPointCollectionFromSource(0, shift(rect), profiler);

	reverse(*result);
	reverseElements(*result);

	return result;
}

auto TimeShiftOperator::getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<LineCollection> {
	auto result = getLineCollectionFromSource(0, shift(rect), profiler);

	reverse(*result);
	reverseElements(*result);

	return result;
}

auto TimeShiftOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) -> std::unique_ptr<PolygonCollection> {
	auto result = getPolygonCollectionFromSource(0, shift(rect), profiler);

	reverse(*result);
	reverseElements(*result);

	return result;
}
