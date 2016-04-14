
#include "datatypes/raster.h"
#include "datatypes/plots/histogram.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <memory>
#include <cmath>
#include <json/json.h>

/**
 * Available modes to specify a range
 */
enum class RangeMode {
	MINMAX, // user specified min/max
	UNIT,  // take min/max from unit
	DATA  // take min/max of data
};

/**
 * This operator computes a equi-width histogram on a given raster or feature collection
 *
 * Params are configured as follow:
 *   - attribute: name of the (numeric) attribute to compute the histogram on. Ignored for operation on rasters
 *   - range: the range on which to compute the histogram on. Must be either one of
 *     - [min, max] array of min and max value
 *     - "unit" String value to use the min/max values of the unit corresponding to raster/feature attribute
 *       throws error if unit is unknown
 *     - "data" String value to compute min/max based on the given raster/feature collection data
 *   - buckets: the number of buckets, can be omitted for integral types (then it is estimated via square root of elements).
 */
class HistogramOperator : public GenericOperator {
	public:
		HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~HistogramOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		RangeMode rangeMode;
		std::string attribute;
		unsigned int buckets;
		double min, max;
};


HistogramOperator::HistogramOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	attribute = params.get("attribute", "").asString();

	if(!params.isMember("range"))
		throw ArgumentException("HistogramOperator: Must specify range");

	if(params["range"].isArray()) {
		rangeMode = RangeMode::MINMAX;
		min = params["range"].get(0U, 0).asDouble();
		max = params["range"].get(1, 0).asDouble();

		if(max <= min)
			throw ArgumentException("HistogramOperator: Invalid range, max must be greater than min");
	} else if (params["range"].asString() == "unit")
		rangeMode = RangeMode::UNIT;
	else if (params["range"].asString() == "data")
		rangeMode = RangeMode::DATA;
	else
		throw ArgumentException("HistogramOperator: Invalid range");

	buckets = params.get("buckets", 0).asUInt();
}

void HistogramOperator::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value params(Json::ValueType::objectValue);

	if(attribute != "")
		params["attribute"] = attribute;

	if(rangeMode == RangeMode::MINMAX) {
		Json::Value range;
		range[0] = min;
		range[1] = max;
		params["range"] = range;
	} else if (rangeMode == RangeMode::UNIT)
		params["range"] = "unit";
	else
		params["range"] = "data";

	if(buckets != 0)
		params["buckets"] = buckets;

	Json::FastWriter writer;
	stream << writer.write(params);
}
HistogramOperator::~HistogramOperator() {
}
REGISTER_OPERATOR(HistogramOperator, "histogram");


#ifndef MAPPING_OPERATOR_STUBS
template<typename T>
struct histogram{
	static std::unique_ptr<GenericPlot> execute(Raster2D<T> *raster, RangeMode rangeMode, T min, T max, size_t buckets) {
		raster->setRepresentation(GenericRaster::Representation::CPU);


		switch(rangeMode){
		case RangeMode::MINMAX:
			break;

		case RangeMode::DATA: {
			min = std::numeric_limits<T>::max();
			max = std::numeric_limits<T>::min();

			int size = raster->getPixelCount();
			for (int i=0;i<size;i++) {
				T v = raster->data[i];
				if (!raster->dd.is_no_data(v)){
					min = std::min(v, min);
					max = std::max(v, max);
				}
			}
			break;
		}

		case RangeMode::UNIT: {
			auto unit = raster->dd.unit;
			if(unit.hasMinMax()){
				min = unit.getMin();
				max = unit.getMax();
			} else {
				min = std::numeric_limits<T>::min();
				max = std::numeric_limits<T>::max();
				//or throw error?
			}
		}
		}

		if(buckets == 0) {
			//estimate via square root
			buckets = sqrt(raster->width * raster->height);

			//upper limit for discrete types
			if(std::is_integral<T>()) {
				buckets = std::min((size_t)(max - min + 1), buckets);
			}
		}

		auto histogram = make_unique<Histogram>(buckets, min, max);

		int size = raster->getPixelCount();
		for (int i=0;i<size;i++) {
			T v = raster->data[i];
			if (raster->dd.is_no_data(v))
				histogram->incNoData();
			else {
				histogram->inc(v);
			}
		}

		return std::unique_ptr<GenericPlot>(std::move(histogram));
	}
};

std::unique_ptr<GenericPlot> createHistogram(SimpleFeatureCollection &features, std::string attribute, RangeMode rangeMode, double min, double max, size_t buckets){
	auto &valueVector = features.feature_attributes.numeric(attribute);
	size_t featureCount = features.getFeatureCount();

	switch(rangeMode){
	case RangeMode::MINMAX:
		break;

	case RangeMode::DATA:
		min = std::numeric_limits<double>::max();
		max = std::numeric_limits<double>::min();

		for (size_t i=0; i < featureCount; i++) {
			double v = valueVector.get(i);
			if (!std::isnan(v)) {
				min = std::min(v, min);
				max = std::max(v, max);
			}
		}
		break;

	case RangeMode::UNIT:
		auto unit = valueVector.unit;
		if(unit.hasMinMax()){
			min = unit.getMin();
			max = unit.getMax();
		} else {
			min = std::numeric_limits<double>::min();
			max = std::numeric_limits<double>::max();
			//or throw error?
		}
	}

	if(buckets == 0) {
		//estimate via square root
		buckets = sqrt(featureCount);
	}

	auto histogram = make_unique<Histogram>(buckets, min, max);
	for (size_t i=0; i < featureCount; i++) {
		double value = valueVector.get(i);
		if (std::isnan(value) /* is NaN */)
			histogram->incNoData();
		else {
			histogram->inc(value);
		}
	}

	return std::unique_ptr<GenericPlot>(std::move(histogram));
}

std::unique_ptr<GenericPlot> HistogramOperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	Profiler::Profiler p("HISTOGRAM_OPERATOR");

	if(getRasterSourceCount() == 1) {
		auto raster = getRasterFromSource(0, rect, profiler);

		return callUnaryOperatorFunc<histogram>(raster.get(), rangeMode, min, max, buckets);
	} else {
		std::unique_ptr<SimpleFeatureCollection> features;
		if(getPointCollectionSourceCount() == 1)
			features = getPointCollectionFromSource(0, rect, profiler);
		if(getLineCollectionSourceCount() == 1)
			features = getLineCollectionFromSource(0, rect, profiler);
		if(getPolygonCollectionSourceCount() == 1)
			features = getPolygonCollectionFromSource(0, rect, profiler);

		return createHistogram(*features, attribute, rangeMode, min, max, buckets);
	}
}
#endif
