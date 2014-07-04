
#include "raster/raster.h"
#include "raster/histogram.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
//#include <algorithm>
#include <json/json.h>


class HistogramOperator : public GenericOperator {
	public:
		HistogramOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~HistogramOperator();

		virtual Histogram *getHistogram(const QueryRectangle &rect);
};


HistogramOperator::HistogramOperator(int sourcecount, GenericOperator *sources[], Json::Value &) : GenericOperator(Type::HISTOGRAM, sourcecount, sources) {
	assumeSources(1);
}
HistogramOperator::~HistogramOperator() {
}
REGISTER_OPERATOR(HistogramOperator, "histogram");

template<typename T>
struct histogram{
	static Histogram *execute(Raster2D<T> *raster) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		auto range = RasterTypeInfo<T>::getRange(min, max);

		std::unique_ptr<Histogram> histogram( new Histogram(range, min, max) );

		int size = raster->lcrs.getPixelCount();
		for (int i=0;i<size;i++) {
			T v = raster->data[i];
			if (raster->dd.is_no_data(v))
				histogram->nodata_count++;
			else {
				uint32_t value = (v - min);
				if (value >= 0 && value < range)
					histogram->counts[value]++;
			}
		}

		return histogram.release();
	}
};


Histogram *HistogramOperator::getHistogram(const QueryRectangle &rect) {
	std::unique_ptr<GenericRaster> raster(sources[0]->getRaster(rect));

	Profiler::Profiler p("HISTOGRAM_OPERATOR");
	return callUnaryOperatorFunc<histogram>(raster.get());
}



