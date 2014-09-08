#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "plot/histogram.h"

#include <string>
#include <json/json.h>


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

	double raster_max = points->getGlobalMDValue(name + "_max");
	double raster_min = points->getGlobalMDValue(name + "_min");
	double raster_no_data = points->getGlobalMDValue(name + "_no_data");
	bool raster_has_no_data = points->getGlobalMDValue(name + "_has_no_data");

	auto histogram = std::make_unique<Histogram>(numberOfBuckets, raster_min, raster_max);

	for (Point &point : points->collection) {
		double value = points->getLocalMDValue(point, name);
		if (raster_has_no_data && value == raster_no_data)
			histogram->incNoData();
		else {
			histogram->inc(value);
		}
	}

	return std::unique_ptr<GenericPlot>(std::move(histogram));
}
