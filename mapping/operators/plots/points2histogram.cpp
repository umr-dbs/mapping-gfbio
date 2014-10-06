#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "plot/histogram.h"

#include <string>
#include <json/json.h>
#include <cmath>

class Points2HistogramOperator : public GenericOperator {
private:
	std::string name;
	unsigned int numberOfBuckets;
public:
	Points2HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
	virtual ~Points2HistogramOperator();

	virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect);
};

Points2HistogramOperator::Points2HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	name = params.get("name", "raster").asString();
	numberOfBuckets = params.get("numberOfBuckets", Histogram::DEFAULT_NUMBER_OF_BUCKETS).asUInt();
}

Points2HistogramOperator::~Points2HistogramOperator() {
}
REGISTER_OPERATOR(Points2HistogramOperator, "points2histogram");


std::unique_ptr<GenericPlot> Points2HistogramOperator::getPlot(const QueryRectangle &rect) {
	auto points = getPointsFromSource(0, rect);

	double raster_max = points->global_md_value.get(name + "_max");
	double raster_min = points->global_md_value.get(name + "_min");
	double raster_no_data = points->global_md_value.get(name + "_no_data");
	bool raster_has_no_data = points->global_md_value.get(name + "_has_no_data");

	auto histogram = std::make_unique<Histogram>(numberOfBuckets, raster_min, raster_max);

	size_t count = points->collection.size();
	auto &vec = points->local_md_value.getVector(name);
	for (size_t idx=0;idx<count;idx++) {
		double value = vec[idx];
		if ((raster_has_no_data && value == raster_no_data) // no data
				|| std::isnan(value) /* is NaN */)
			histogram->incNoData();
		else {
			histogram->inc(value);
		}
	}

	return std::unique_ptr<GenericPlot>(std::move(histogram));
}
