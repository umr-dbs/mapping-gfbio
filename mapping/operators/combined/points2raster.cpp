
#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/pointcollection.h"
#include "raster/typejuggling.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <algorithm>

class PointsToRasterOperator : public GenericOperator {
	public:
	PointsToRasterOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~PointsToRasterOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};




PointsToRasterOperator::PointsToRasterOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}

PointsToRasterOperator::~PointsToRasterOperator() {
}
REGISTER_OPERATOR(PointsToRasterOperator, "points2raster");


GenericRaster *PointsToRasterOperator::getRaster(const QueryRectangle &rect) {

	const int MAX = 255;
	const int RADIUS = 8;
	typedef uint8_t T;


	QueryRectangle rect2 = rect;
	rect2.enlarge(RADIUS);
	PointCollection *points = sources[0]->getPoints(rect2);
	std::unique_ptr<PointCollection> points_guard(points);

	LocalCRS rm(rect);
	DataDescription vm(GDT_Byte, 0, MAX, true, 0);
	auto raster_out_guard = GenericRaster::create(rm, vm, GenericRaster::Representation::CPU);
	Raster2D<T> *raster_out = (Raster2D<T> *) raster_out_guard.get();

	raster_out->clear(0);
	for (Point &p : points->collection) {
		double x = p.x, y = p.y;

		int px = floor(rm.WorldToPixelX(x));
		int py = floor(rm.WorldToPixelY(y));

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

	return raster_out_guard.release();
}
