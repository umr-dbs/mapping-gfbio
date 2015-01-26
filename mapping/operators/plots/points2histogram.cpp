#include "datatypes/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "datatypes/plots/histogram.h"

#include <string>
#include <json/json.h>
#include <cmath>
#include <limits>		// std::numeric_limits

/**
 * This class generates a histogram out of a point set with attached meta data.
 */
class Points2HistogramOperator : public GenericOperator {
private:
	std::string name;
	unsigned int numberOfBuckets;
	double rangeMin, rangeMax;
	bool autoRange;
public:
	Points2HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
	virtual ~Points2HistogramOperator();

	virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);
};

/**
 * The constructor takes the meta data field as parameter and the range.
 * If autorange is set to true the range will be calculated automatically.
 */
Points2HistogramOperator::Points2HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	name = params.get("name", "raster").asString();
	numberOfBuckets = params.get("numberOfBuckets", Histogram::DEFAULT_NUMBER_OF_BUCKETS).asUInt();

	autoRange = params.get("autoRange", true).asBool();
	if(!autoRange) {
		rangeMin = params.get("rangeMin", std::numeric_limits<double>::min()).asDouble();
		rangeMin = params.get("rangeMin", std::numeric_limits<double>::max()).asDouble();

		// fallback if range is invalid or 0
		if(rangeMax <= rangeMin) {
			autoRange = true;
		}
	}
}

Points2HistogramOperator::~Points2HistogramOperator() {
}
REGISTER_OPERATOR(Points2HistogramOperator, "points2histogram");


/**
 * Calculates the histogram and returns it.
 */
std::unique_ptr<GenericPlot> Points2HistogramOperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointsFromSource(0, rect, profiler);

	//double raster_max = points->global_md_value.get(name + "_max");
	//double raster_min = points->global_md_value.get(name + "_min");

	size_t pointSize = points->collection.size();
	auto& valueVector = points->local_md_value.getVector(name);

	// detect range automatically
	if(autoRange) {
		rangeMin = std::numeric_limits<double>::max();
		rangeMax = std::numeric_limits<double>::min();

		for (size_t i=0; i < pointSize; i++) {
			double value = valueVector[i];
			if (!std::isnan(value) /* is no NaN */) {
				if(value > rangeMax) {
					rangeMax = value;
				}
				if(value < rangeMin) {
					rangeMin = value;
				}
			}
		}
	}

	auto histogram = std::make_unique<Histogram>(numberOfBuckets, rangeMin, rangeMax);

	for (size_t i=0; i< pointSize; i++) {
		double value = valueVector[i];
		if (std::isnan(value) /* is NaN */)
			histogram->incNoData();
		else {
			histogram->inc(value);
		}
	}

	return std::unique_ptr<GenericPlot>(std::move(histogram));
}
