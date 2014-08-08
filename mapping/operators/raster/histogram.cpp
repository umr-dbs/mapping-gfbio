
#include "raster/raster.h"
#include "raster/histogram.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <memory>
#include <cmath>
#include <json/json.h>


class HistogramOperator : public GenericOperator {
	public:
		HistogramOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~HistogramOperator();

		virtual std::unique_ptr<DataVector> getDataVector(const QueryRectangle &rect);
};


HistogramOperator::HistogramOperator(int sourcecount, GenericOperator *sources[], Json::Value &) : GenericOperator(Type::DATAVECTOR, sourcecount, sources) {
	assumeSources(1);
}
HistogramOperator::~HistogramOperator() {
}
REGISTER_OPERATOR(HistogramOperator, "histogram");


template<typename T>
struct histogram{
	static std::unique_ptr<DataVector> execute(Raster2D<T> *raster) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		auto range = RasterTypeInfo<T>::getRange(min, max);
		auto histogram = std::make_unique<Histogram>(range, min, max);

		int size = raster->lcrs.getPixelCount();
		for (int i=0;i<size;i++) {
			T v = raster->data[i];
			if (raster->dd.is_no_data(v))
				histogram->incNoData();
			else {
				histogram->inc(v);
			}
		}

		return std::unique_ptr<DataVector>(std::move(histogram));
	}
};


std::unique_ptr<DataVector> HistogramOperator::getDataVector(const QueryRectangle &rect) {
	auto raster = sources[0]->getRaster(rect);

	Profiler::Profiler p("HISTOGRAM_OPERATOR");
	return callUnaryOperatorFunc<histogram>(raster.get());
}
