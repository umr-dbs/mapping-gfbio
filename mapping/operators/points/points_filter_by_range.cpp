#include "datatypes/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <json/json.h>
#include <limits>

class PointsFilterByRangeOperator: public GenericOperator {
private:
	std::string name;
	bool includeNoData;
	double rangeMin, rangeMax;
public:
	PointsFilterByRangeOperator(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsFilterByRangeOperator();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect, QueryProfiler &profiler);
};

PointsFilterByRangeOperator::PointsFilterByRangeOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	name = params.get("name", "raster").asString();
	includeNoData = params.get("includeNoData", false).asBool();
	rangeMin = params.get("rangeMin", std::numeric_limits<double>::min()).asDouble();
	rangeMax = params.get("rangeMax", std::numeric_limits<double>::max()).asDouble();
}

PointsFilterByRangeOperator::~PointsFilterByRangeOperator() {
}
REGISTER_OPERATOR(PointsFilterByRangeOperator, "points_filter_by_range");

std::unique_ptr<PointCollection> PointsFilterByRangeOperator::getPoints(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointsFromSource(0, rect, profiler);

	size_t count = points->collection.size();
	std::vector<bool> keep(count, false);

	for (size_t idx=0;idx<count;idx++) {
		double value = points->local_md_value.get(idx, name);
		bool copy = false;

		if (std::isnan(value)) {
			copy = includeNoData;
		}
		else {
			copy = (value >= rangeMin && value <= rangeMax);
		}

		keep[idx] = copy;
	}

	return points->filter(keep);
}
