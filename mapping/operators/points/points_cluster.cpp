#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "pointvisualization/CircleClusteringQuadTree.h"

#include <string>
#include <json/json.h>

class PointsClusterOperator: public GenericOperator {
private:

public:
	PointsClusterOperator(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsClusterOperator();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};

PointsClusterOperator::PointsClusterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}

PointsClusterOperator::~PointsClusterOperator() {
}
REGISTER_OPERATOR(PointsClusterOperator, "points_cluster");

std::unique_ptr<PointCollection> PointsClusterOperator::getPoints(const QueryRectangle &rect) {
	// TODO: EXPECT EPSG:3857

	auto pointsOld = getPointsFromSource(0, rect);
	auto pointsNew = std::make_unique<PointCollection>(rect.epsg);

	//PointCollectionMetadataCopier metadataCopier(*pointsOld, *pointsNew);
	//metadataCopier.copyGlobalMetadata();
	//metadataCopier.initLocalMetadataFields();

	pv::CircleClusteringQuadTree clusterer(pv::BoundingBox(
													pv::Coordinate((rect.x2 + rect.x1) / (2 * rect.xres), (rect.y2 + rect.y2) / (2 * rect.yres)),
													pv::Dimension((rect.x2 - rect.x1) / (2 * rect.xres), (rect.y2 - rect.y2) / (2 * rect.yres)),
												1), 1);
	for (Point &pointOld : pointsOld->collection) {
		clusterer.insert(std::make_shared<pv::Circle>(pv::Coordinate(pointOld.x / rect.xres, pointOld.y / rect.yres), 5, 1));
	}

	pointsNew->addLocalMDValue("radius");
	pointsNew->addLocalMDValue("numberOfPoints");
	for (auto& circle : clusterer.getCircles()) {
		Point& pointNew = pointsNew->addPoint(circle->getX() * rect.xres, circle->getY() * rect.yres);
		//metadataCopier.copyLocalMetadata(pointOld, pointNew);
		pointsNew->setLocalMDValue(pointNew, "radius", circle->getRadius());
		pointsNew->setLocalMDValue(pointNew, "numberOfPoints", circle->getNumberOfPoints());
	}

	return pointsNew;
}
