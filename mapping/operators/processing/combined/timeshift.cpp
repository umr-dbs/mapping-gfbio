#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <ctime>
#include <algorithm>
#include <json/json.h>

#include "datatypes/raster.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/spatiotemporal.h"

#include "util/exceptions.h"

#include "util/timemodification.h"

/**
 * Operator that modifies the temporal dimension of a query rectangle.
 * It allows the modification of the validity of results, to combine data with different temporal validity
 */
class TimeShiftOperator : public GenericOperator {
	public:
		TimeShiftOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~TimeShiftOperator();

		virtual auto getRaster(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<GenericRaster>;
		virtual auto getPointCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<PointCollection>;
		virtual auto getLineCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<LineCollection>;
		virtual auto getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<PolygonCollection>;

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		/**
		 * Creates the time modification.
		 *
		 * @param temporal_reference The temporal reference of the query.
		 */
		auto createTimeModification(const TemporalReference& temporal_reference) -> TimeModification;

		/**
		 * Shift a {@ref QueryRectangle} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param rect a query rectangle reference
		 *
		 * @return a shifted query rectagle
		 */
		inline auto shift(TimeModification& time_modification, const QueryRectangle& rect) -> const QueryRectangle;

		/**
		 * Reverses the shift on a {@ref SpatioTemporalResult} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param result A spatio temporal result reference
		 */
		inline auto reverse(TimeModification& time_modification, SpatioTemporalResult& result) -> void;

		/**
		 * Reverses the shift on the elements of a {@ref SimpleFeatureCollection} by using the time_modification.
		 *
		 * @param time_modification A TimeModification object.
		 * @param collection A simple feature collection
		 */
		inline auto reverseElements(TimeModification& time_modification, SimpleFeatureCollection& collection) -> void;

		bool shift_has_from = false, shift_has_to = false;
		std::string shift_from_unit, shift_from_value;
		std::string shift_to_unit, shift_to_value;
		bool has_stretch = false;
		int stretch_factor;
		std::string stretch_fixed_point;
		bool snap_has_from = false, snap_has_to = false;
		std::string snap_from_unit, snap_to_unit;
		int snap_from_value, snap_to_value;
		bool snap_from_allow_reset, snap_to_allow_reset;
};

void TimeShiftOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	bool has_shift = shift_has_from || shift_has_to;
	bool has_snap = snap_has_from || snap_has_to;

	std::string shift_from_quote = (shift_from_unit == "absolute") ? "\"" : "";
	std::string shift_to_quote = (shift_to_unit == "absolute") ? "\"" : "";

	if(has_shift) {
		stream << "\"shift\":{";
		if(shift_has_from) {
			stream << "\"from\":{\"unit\":\"" << shift_from_unit << "\",\"value\":" << shift_from_quote << shift_from_value << shift_from_quote << "}";
			if(shift_has_to) {
				stream << ",";
			}
		}
		if(shift_has_to) {
			stream << "\"to\":{\"unit\":\"" << shift_to_unit << "\",\"value\":" << shift_to_quote << shift_to_value << shift_to_quote << "}";
		}
		stream << "}";
		if(has_stretch || has_snap) {
			stream << ",";
		}
	}
	if(has_stretch) {
		stream << "\"stretch\":{";
		stream << "\"factor\":" << stretch_factor << ",\"fixedPoint\":\"" << stretch_fixed_point << "\"";
		stream << "}";
		if(has_snap) {
			stream << ",";
		}
	}
	if(has_snap) {
		stream << "\"snap\":{";
		if(snap_has_from) {
			stream << "\"from\":{\"unit\":\"" << snap_from_unit << "\",\"value\":" << snap_from_value << ",\"allowReset\":" << snap_from_allow_reset << "}";
			if(shift_has_to) {
				stream << ",";
			}
		}
		if(snap_has_to) {
			stream << "\"to\":{\"unit\":\"" << snap_to_unit << "\",\"value\":" << snap_to_value << ",\"allowReset\":" << snap_to_allow_reset << "}";
		}
		stream << "}";
	}
	stream << "}";
}

TimeShiftOperator::TimeShiftOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	auto toLowercase = [] (std::string value) -> std::string {
		std::transform(value.begin(), value.end(), value.begin(), static_cast<int(*)(int)>(std::tolower));
		return value;
	};

	// process shift parameters
	auto shift_parameter = params.get("shift", Json::nullValue);
	if (!shift_parameter.isNull() && shift_parameter.isMember("from")) {
		auto shift_from_parameter = shift_parameter.get("from", Json::nullValue);
		shift_has_from = true;

		shift_from_unit = toLowercase(shift_from_parameter.get("unit", "none").asString());
		if(shift_from_unit == "none") {
			throw ArgumentException("Unit must not be <none>.");
		}

		auto shift_from_value_parameter = shift_from_parameter.get("value", "");
		if(shift_from_value_parameter.isInt()) {
			shift_from_value = std::to_string(shift_from_value_parameter.asInt());
		} else {
			shift_from_value = shift_from_value_parameter.asString();
		}
		if(shift_from_value == "") {
			throw ArgumentException("Shift value must not be <empty>.");
		}
	}
	if (!shift_parameter.isNull() && shift_parameter.isMember("to")) {
		auto shift_to_parameter = shift_parameter.get("to", Json::nullValue);
		shift_has_to = true;

		shift_to_unit = toLowercase(shift_to_parameter.get("unit", "none").asString());
		if(shift_to_unit == "none") {
			throw ArgumentException("Unit must not be <none>.");
		}

		auto shift_to_value_parameter = shift_to_parameter.get("value", "");
		if(shift_to_value_parameter.isInt()) {
			shift_to_value = std::to_string(shift_to_value_parameter.asInt());
		} else {
			shift_to_value = shift_to_value_parameter.asString();
		}
		if(shift_to_value == "") {
			throw ArgumentException("Shift value must not be <empty>.");
		}
	}

	// process stretch parameters
	auto stretch_parameter = params.get("stretch", Json::nullValue);
	if (!stretch_parameter.isNull()) {
		has_stretch = true;

		stretch_fixed_point = toLowercase(stretch_parameter.get("fixedPoint", "center").asString());

		stretch_factor = stretch_parameter.get("factor", 1).asInt();
	}

	// process snap parameters
	auto snap_parameter = params.get("snap", Json::nullValue);
	if (!snap_parameter.isNull() && snap_parameter.isMember("from")) {
		auto snap_from_parameter = snap_parameter.get("from", Json::nullValue);
		snap_has_from = true;

		snap_from_unit = snap_from_parameter.get("unit", "none").asString();
		if(snap_from_unit == "none") {
			throw ArgumentException("Unit must not be <none>.");
		}

		snap_from_value = snap_from_parameter.get("value", -1).asInt();
		if(snap_from_value < 0) {
			throw ArgumentException("Snap value must not be <empty>.");
		}

		snap_from_allow_reset = snap_from_parameter.get("allowReset", false).asBool();
	}
	if (!snap_parameter.isNull() && snap_parameter.isMember("to")) {
		auto snap_to_parameter = snap_parameter.get("to", Json::nullValue);
		snap_has_to = true;

		snap_to_unit = snap_to_parameter.get("unit", "none").asString();
		if(snap_to_unit == "none") {
			throw ArgumentException("Unit must not be <none>.");
		}

		snap_to_value = snap_to_parameter.get("value", -1).asInt();
		if(snap_to_value < 0) {
			throw ArgumentException("Snap value must not be <empty>.");
		}

		snap_to_allow_reset = snap_to_parameter.get("allowReset", false).asBool();
	}

}

TimeShiftOperator::~TimeShiftOperator() {}
REGISTER_OPERATOR(TimeShiftOperator, "timeshift");

auto TimeShiftOperator::createTimeModification(const TemporalReference& temporal_reference) -> TimeModification {
	std::unique_ptr<TimeShift> shift_from, shift_to;
	std::unique_ptr<TimeShift> stretch;
	std::unique_ptr<TimeShift> snap_from, snap_to;

	if(shift_has_from) {
		if(shift_from_unit == "absolute") {
			shift_from = make_unique<AbsoluteShift>(boost::posix_time::time_from_string(shift_from_value));
		} else {
			shift_from = make_unique<RelativeShift>(std::stoi(shift_from_value), RelativeShift::createUnit(shift_from_unit));
		}
	} else {
		shift_from = make_unique<Identity>();
	}

	if(shift_has_to) {
		if(shift_to_unit == "absolute") {
			shift_to = make_unique<AbsoluteShift>(boost::posix_time::time_from_string(shift_to_value));
		} else {
			shift_to = make_unique<RelativeShift>(std::stoi(shift_to_value), RelativeShift::createUnit(shift_to_unit));
		}
	} else {
		shift_to = make_unique<Identity>();
	}

	if(has_stretch) {
		if(stretch_fixed_point == "start") {
			stretch = make_unique<Stretch>(boost::posix_time::from_time_t(static_cast<time_t>(temporal_reference.t1)) ,stretch_factor);
		} else if(stretch_fixed_point == "end") {
			stretch = make_unique<Stretch>(boost::posix_time::from_time_t(static_cast<time_t>(temporal_reference.t2)), stretch_factor);
		} else {
			// center
			auto center = boost::posix_time::from_time_t(static_cast<time_t>(temporal_reference.t1 + temporal_reference.t2) / 2);
			stretch = make_unique<Stretch>(center, stretch_factor);
		}
	} else {
		stretch = make_unique<Identity>();
	}

	if(snap_has_from) {
		snap_from = make_unique<Snap>(Snap::createUnit(snap_from_unit), snap_from_value, snap_from_allow_reset);
	} else {
		snap_from = make_unique<Identity>();
	}

	if(snap_has_to) {
		snap_to = make_unique<Snap>(Snap::createUnit(snap_to_unit), snap_to_value, snap_to_allow_reset);
	} else {
		snap_to = make_unique<Identity>();
	}

	return TimeModification{std::move(shift_from), std::move(shift_to), std::move(stretch), std::move(snap_from), std::move(snap_to)};
}

inline auto TimeShiftOperator::shift(TimeModification& time_modification, const QueryRectangle &rect) -> const QueryRectangle {
	TemporalReference modified_temporal_reference = time_modification.apply(rect);

	return QueryRectangle{rect, modified_temporal_reference, rect};
}

inline auto TimeShiftOperator::reverse(TimeModification& time_modification, SpatioTemporalResult& result) -> void {
	SpatioTemporalReference result_stref{result.stref, time_modification.reverse(result.stref)};
	result.replaceSTRef(result_stref);
}

inline auto TimeShiftOperator::reverseElements(TimeModification& time_modification, SimpleFeatureCollection& collection) -> void {
	if(!collection.hasTime()) {
		collection.addDefaultTimestamps();
	}

	for(size_t i = 0; i < collection.getFeatureCount(); ++i) {
		TemporalReference feature_tref{TIMETYPE_UNIX, static_cast<double>(collection.time[i].t1), static_cast<double>(collection.time[i].t2)};
		TemporalReference result_point_tref = time_modification.reverse(feature_tref);
		collection.time[i].t1 = result_point_tref.t1;
		collection.time[i].t2 = result_point_tref.t2;
	}
}

auto TimeShiftOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<GenericRaster> {
	TimeModification time_modification = createTimeModification(rect);
	auto result = getRasterFromSource(0, shift(time_modification, rect), tools);

	reverse(time_modification, *result);

	return result;
}

auto TimeShiftOperator::getPointCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<PointCollection> {
	TimeModification time_modification = createTimeModification(rect);
	auto result = getPointCollectionFromSource(0, shift(time_modification, rect), tools);

	reverse(time_modification, *result);
	reverseElements(time_modification, *result);

	return result;
}

auto TimeShiftOperator::getLineCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<LineCollection> {
	TimeModification time_modification = createTimeModification(rect);
	auto result = getLineCollectionFromSource(0, shift(time_modification, rect), tools);

	reverse(time_modification, *result);
	reverseElements(time_modification, *result);

	return result;
}

auto TimeShiftOperator::getPolygonCollection(const QueryRectangle &rect, const QueryTools &tools) -> std::unique_ptr<PolygonCollection> {
	TimeModification time_modification = createTimeModification(rect);
	auto result = getPolygonCollectionFromSource(0, shift(time_modification, rect), tools);

	reverse(time_modification, *result);
	reverseElements(time_modification, *result);

	return result;
}
