#include "util/exceptions.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "datatypes/plots/histogram.h"

#include <string>
#include <json/json.h>
#include <cmath>
#include <limits>		// std::numeric_limits
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"

/**
 * This class generates a histogram out of a feature set with attached attributes.
 */
class HistogramFromFeaturesOperator : public GenericOperator {
	public:
		HistogramFromFeaturesOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~HistogramFromFeaturesOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string attributename;
		unsigned int numberOfBuckets;
		double rangeMin, rangeMax;
		bool autoRange;
};

/**
 * The constructor takes the meta data field as parameter and the range.
 * If autorange is set to true the range will be calculated automatically.
 */
HistogramFromFeaturesOperator::HistogramFromFeaturesOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	rangeMin = rangeMax = 0;
	attributename = params.get("name", "").asString();
	numberOfBuckets = params.get("numberOfBuckets", Histogram::DEFAULT_NUMBER_OF_BUCKETS).asUInt();

	autoRange = params.get("autoRange", true).asBool();
	if(!autoRange) {
		rangeMin = params.get("rangeMin", std::numeric_limits<double>::min()).asDouble();
		rangeMin = params.get("rangeMin", std::numeric_limits<double>::max()).asDouble();

		// fallback if range is invalid or 0
		if(rangeMax <= rangeMin) {
			throw ArgumentException("HistogramFromFeaturesOperator: rangeMin must be smaller than rangeMax");
		}
	}
}

HistogramFromFeaturesOperator::~HistogramFromFeaturesOperator() {
}
REGISTER_OPERATOR(HistogramFromFeaturesOperator, "histogram_from_features");

void HistogramFromFeaturesOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"name\":\"" << attributename << "\","
			<< "\"numberOfBuckets\":" << numberOfBuckets << ",";
	if(autoRange) {
		stream << "\"autoRange\":true";
	} else {
		stream << "\"autoRange\":false, \"rangeMin\":" << rangeMin << ",\"rangeMax\":" << rangeMax;
	}
}


#ifndef MAPPING_OPERATOR_STUBS
/**
 * Calculates the histogram and returns it.
 */
std::unique_ptr<GenericPlot> HistogramFromFeaturesOperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	std::unique_ptr<SimpleFeatureCollection> features;
	if (getPointCollectionSourceCount() > 0) {
		features = getPointCollectionFromSource(0, rect, profiler);
	}
	else if (getLineCollectionSourceCount() > 0) {
		features = getLineCollectionFromSource(0, rect, profiler);
	}
	else if (getPolygonCollectionSourceCount() > 0) {
		features = getPolygonCollectionFromSource(0, rect, profiler);
	}
	else
		throw OperatorException("HistogramFromFeaturesOperator: need a source");


	size_t featurecount = features->getFeatureCount();
	auto &valueVector = features->local_md_value.getVector(attributename);

	// detect range automatically
	if (autoRange) {
		rangeMin = std::numeric_limits<double>::max();
		rangeMax = std::numeric_limits<double>::min();

		for (size_t i=0; i < featurecount; i++) {
			double value = valueVector[i];
			if (!std::isnan(value)) {
				rangeMin = std::min(value, rangeMin);
				rangeMax = std::max(value, rangeMax);
			}
		}
	}

	auto histogram = make_unique<Histogram>(numberOfBuckets, rangeMin, rangeMax);
	for (size_t i=0; i < featurecount; i++) {
		double value = valueVector[i];
		if (std::isnan(value) /* is NaN */)
			histogram->incNoData();
		else {
			histogram->inc(value);
		}
	}

	return std::unique_ptr<GenericPlot>(std::move(histogram));
}
#endif
