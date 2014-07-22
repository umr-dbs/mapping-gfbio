#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/histogram.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/pointcollection.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <json/json.h>

class PointRasterHistogramOperator: public GenericOperator {
private:
	unsigned int numberOfBuckets;
public:
	PointRasterHistogramOperator(int sourcecount, GenericOperator *sources[],
			Json::Value &params);
	virtual ~PointRasterHistogramOperator();

	virtual Histogram *getHistogram(const QueryRectangle &rect);
};

PointRasterHistogramOperator::PointRasterHistogramOperator(int sourcecount,
		GenericOperator *sources[], Json::Value &params) :
		GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(2);

	numberOfBuckets = params.get("numberOfBuckets", UINT_MAX).asUInt();
}

PointRasterHistogramOperator::~PointRasterHistogramOperator() {
}
REGISTER_OPERATOR(PointRasterHistogramOperator, "pointRasterHistogram");

template<typename T>
struct pointRasterHistogram {
	static Histogram* execute(Raster2D<T>* raster, PointCollection* points, unsigned int numberOfBuckets) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		auto range = RasterTypeInfo<T>::getRange(min, max);

		if(range < numberOfBuckets) {
			numberOfBuckets = range;
		}
		std::unique_ptr<Histogram> histogram(new Histogram(numberOfBuckets, min, max));

		for (Point &p : points->collection) {
			double x = p.x, y = p.y;

			int px = floor(raster->lcrs.WorldToPixelX(x));
			int py = floor(raster->lcrs.WorldToPixelY(y));

			if (px >= 0 && py >= 0 && (size_t) px < raster->lcrs.size[0]
					&& (size_t) py < raster->lcrs.size[1]) {
				T value = raster->get(px, py);

				if (raster->dd.is_no_data(value))
					histogram->addNoDataEntry();
				else {
					histogram->add(value);
				}
			}
		}

		return histogram.release();
	}
};

Histogram *PointRasterHistogramOperator::getHistogram(
		const QueryRectangle &rect) {
	std::unique_ptr<PointCollection> points(sources[0]->getPoints(rect));
	std::unique_ptr<GenericRaster> raster(sources[1]->getRaster(rect));

	Profiler::Profiler p("POINT_RASTER_HISTOGRAM_OPERATOR");
	return callUnaryOperatorFunc<pointRasterHistogram>(raster.get(), points.get(), numberOfBuckets);
}
