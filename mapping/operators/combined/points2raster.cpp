
#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/pointcollection.h"
#include "datatypes/raster/typejuggling.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <algorithm>

class PointsToRasterOperator : public GenericOperator {
	public:
		PointsToRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PointsToRasterOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
};




PointsToRasterOperator::PointsToRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}

PointsToRasterOperator::~PointsToRasterOperator() {
}
REGISTER_OPERATOR(PointsToRasterOperator, "points2raster");


std::unique_ptr<GenericRaster> PointsToRasterOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {

	const int MAX = 255;
	const int RADIUS = 8;
	typedef uint8_t T;


	QueryRectangle rect2 = rect;
	rect2.enlarge(RADIUS);

	auto points = getPointsFromSource(0, rect2, profiler);

	DataDescription dd(GDT_Byte, 0, MAX, true, 0);
	auto raster_out_guard = GenericRaster::create(dd, rect, rect.xres, rect.yres, 0, GenericRaster::Representation::CPU);
	Raster2D<T> *raster_out = (Raster2D<T> *) raster_out_guard.get();

	raster_out->clear(0);
	for (Point &p : points->collection) {
		double x = p.x, y = p.y;

		auto px = raster_out->WorldToPixelX(x);
		auto py = raster_out->WorldToPixelY(y);

		for (int dy = -RADIUS;dy <= RADIUS;dy++)
			for (int dx = -RADIUS;dx <= RADIUS;dx++) {
				T oldvalue = raster_out->getSafe(px+dx, py+dy);
				double delta = RADIUS-std::sqrt(dx*dx + dy*dy);
				if (delta <= 0)
					continue;
				T newvalue = std::min((RasterTypeInfo<T>::accumulator_type) MAX, (RasterTypeInfo<T>::accumulator_type) (oldvalue + std::ceil(delta)));
				raster_out->setSafe(px+dx, py+dy, newvalue);
			}
	}

	return raster_out_guard;
}
