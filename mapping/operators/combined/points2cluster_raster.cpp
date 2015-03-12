
#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/pointcollection.h"
#include "datatypes/raster/typejuggling.h"
#include "operators/operator.h"
#include "pointvisualization/CircleClusteringQuadTree.h"

#include <memory>
#include <cmath>
#include <algorithm>

class PointsToClusterRasterOperator : public GenericOperator {
	public:
		PointsToClusterRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PointsToClusterRasterOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
};




PointsToClusterRasterOperator::PointsToClusterRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}

PointsToClusterRasterOperator::~PointsToClusterRasterOperator() {
}
REGISTER_OPERATOR(PointsToClusterRasterOperator, "points2cluster_raster");


std::unique_ptr<GenericRaster> PointsToClusterRasterOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {

	const int MAX = 255;
	typedef uint8_t T;

	std::unique_ptr<PointCollection> points = getPointsFromSource(0, rect, profiler);

	DataDescription dd(GDT_Byte, 0, MAX, true, 0);

	GridSpatioTemporalResult crs(rect, rect.xres, rect.yres);

	pv::CircleClusteringQuadTree clusterer(pv::BoundingBox(pv::Coordinate((rect.x2 + rect.x1) / 2, (rect.y2 + rect.y2) / 2), pv::Dimension((rect.x2 - rect.x1) / 2, (rect.y2 - rect.y2) / 2), 1), 1);
	for (Point &p : points->collection) {
		auto px = crs.WorldToPixelX(p.x);
		auto py = crs.WorldToPixelY(p.y);

		clusterer.insert(std::make_shared<pv::Circle>(pv::Coordinate(px, py), 5, 1));
	}


	std::unique_ptr<GenericRaster> raster_out_guard = GenericRaster::create(dd, crs, GenericRaster::Representation::CPU);
	Raster2D<T> *raster_out = (Raster2D<T> *) raster_out_guard.get();

	raster_out->clear(0);
	for (auto& circle : clusterer.getCircles()) {
		double RADIUS = circle->getRadius();

		for (int dy = -RADIUS;dy <= RADIUS;dy++)
			for (int dx = -RADIUS;dx <= RADIUS;dx++) {
				double delta = RADIUS-std::sqrt(dx*dx + dy*dy);
				if (delta <= 0)
					continue;
				raster_out->setSafe(circle->getX()+dx, circle->getY()+dy, std::min(circle->getNumberOfPoints(), MAX));
			}
	}

	return raster_out_guard;
}
