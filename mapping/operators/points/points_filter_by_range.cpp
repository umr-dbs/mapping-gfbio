#include "raster/pointcollection.h"
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

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
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

std::unique_ptr<PointCollection> PointsFilterByRangeOperator::getPoints(const QueryRectangle &rect) {
	auto points = getPointsFromSource(0, rect);

	size_t count = points->collection.size();
	std::vector<bool> keep(count, false);

	double raster_no_data = points->global_md_value.get(name + "_no_data");
	bool raster_has_no_data = points->global_md_value.get(name + "_has_no_data");

	for (size_t idx=0;idx<count;idx++) {
		double value = points->local_md_value.get(idx, name);
		bool copy = false;

		if ((raster_has_no_data && value == raster_no_data) || std::isnan(value)) {
			copy = includeNoData;
		}
		else {
			copy = (value >= rangeMin && value <= rangeMax);
		}

		keep[idx] = copy;
	}

	return points->filter(keep);
}
