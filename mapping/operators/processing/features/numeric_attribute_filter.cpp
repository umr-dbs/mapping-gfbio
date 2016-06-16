#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"

#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <cmath>

/**
 * Operator that filters a feature collection based on the range of an numeric attribute
 *
 * Parameters:
 * - name: the name of the attribute
 * - includeNoData: boolean whether no data value is kept
 * - rangeMin: the lower bound of the filter
 * - rangeMax the upper bound of the filter
 */
class NumericAttributeFilterOperator: public GenericOperator {
	public:
		NumericAttributeFilterOperator(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
		virtual ~NumericAttributeFilterOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string name;
		bool includeNoData;
		double rangeMin, rangeMax;
	};

NumericAttributeFilterOperator::NumericAttributeFilterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	name = params.get("name", "").asString();
	includeNoData = params.get("includeNoData", false).asBool();
	rangeMin = params.get("rangeMin", std::numeric_limits<double>::min()).asDouble();
	rangeMax = params.get("rangeMax", std::numeric_limits<double>::max()).asDouble();
}

NumericAttributeFilterOperator::~NumericAttributeFilterOperator() {
}
REGISTER_OPERATOR(NumericAttributeFilterOperator, "numeric_attribute_filter");

void NumericAttributeFilterOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	stream << "\"includeNoData\":" << std::boolalpha << includeNoData << ","
			<< "\"name\":\"" << name << "\","
			<< "\"rangeMin\":" << rangeMin << ","
			<< "\"rangeMax\":" << rangeMax;
	stream << "}";
}


#ifndef MAPPING_OPERATOR_STUBS
std::vector<bool> filter(const SimpleFeatureCollection &collection, const std::string &name, double min, double max, bool keepNAN) {
	size_t count = collection.getFeatureCount();
	std::vector<bool> keep(count, false);

	auto &attributes = collection.feature_attributes.numeric(name);

	for (size_t i=0;i<count;i++) {
		double value = attributes.get(i);
		bool copy = false;

		if (std::isnan(value)) {
			copy = keepNAN;
		}
		else {
			copy = (value >= min && value <= max);
		}

		keep[i] = copy;
	}

	return keep;
}

std::unique_ptr<PointCollection> NumericAttributeFilterOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointCollectionFromSource(0, rect, profiler);
	auto keep = filter(*points, name, rangeMin, rangeMax, includeNoData);
	return points->filter(keep);
}

std::unique_ptr<LineCollection> NumericAttributeFilterOperator::getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto lines = getLineCollectionFromSource(0, rect, profiler);
	auto keep = filter(*lines, name, rangeMin, rangeMax, includeNoData);
	return lines->filter(keep);
}
std::unique_ptr<PolygonCollection> NumericAttributeFilterOperator::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto polys = getPolygonCollectionFromSource(0, rect, profiler);
	auto keep = filter(*polys, name, rangeMin, rangeMax, includeNoData);
	return polys->filter(keep);
}
#endif


// obsolete, keep for backwards compatibility for a while
class PointsFilterByRangeOperator : public NumericAttributeFilterOperator {
	public:
		PointsFilterByRangeOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : NumericAttributeFilterOperator(sourcecounts, sources, params) {}
};
REGISTER_OPERATOR(PointsFilterByRangeOperator, "points_filter_by_range");
