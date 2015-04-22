#include "datatypes/multipointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "pointvisualization/CircleClusteringQuadTree.h"

#include <string>
#include <json/json.h>

class PointsClusterOperator: public GenericOperator {
	public:
		PointsClusterOperator(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
		virtual ~PointsClusterOperator();

		virtual std::unique_ptr<MultiPointCollection> getMultiPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
};

PointsClusterOperator::PointsClusterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}

PointsClusterOperator::~PointsClusterOperator() {
}
REGISTER_OPERATOR(PointsClusterOperator, "points_cluster");

std::unique_ptr<MultiPointCollection> PointsClusterOperator::getMultiPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	// TODO: EXPECT EPSG:3857

	auto pointsOld = getMultiPointCollectionFromSource(0, rect, profiler);
	auto pointsNew = std::make_unique<MultiPointCollection>(pointsOld->stref);

	pv::CircleClusteringQuadTree clusterer(pv::BoundingBox(
													pv::Coordinate((rect.x2 + rect.x1) / (2 * rect.xres), (rect.y2 + rect.y2) / (2 * rect.yres)),
													pv::Dimension((rect.x2 - rect.x1) / (2 * rect.xres), (rect.y2 - rect.y2) / (2 * rect.yres)),
												1), 1);
	for (Coordinate &pointOld : pointsOld->coordinates) {
		clusterer.insert(std::make_shared<pv::Circle>(pv::Coordinate(pointOld.x / rect.xres, pointOld.y / rect.yres), 5, 1));
	}

	auto circles = clusterer.getCircles();
	pointsNew->local_md_value.addVector("radius", circles.size());
	pointsNew->local_md_value.addVector("numberOfPoints", circles.size());
	for (auto& circle : circles) {
		size_t idx = pointsNew->addSinglePointFeature(Coordinate(circle->getX() * rect.xres, circle->getY() * rect.yres));
		pointsNew->local_md_value.set(idx, "radius", circle->getRadius());
		pointsNew->local_md_value.set(idx, "numberOfPoints", circle->getNumberOfPoints());
	}

	return pointsNew;
}
