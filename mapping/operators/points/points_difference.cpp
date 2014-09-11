#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <algorithm>
#include <functional>

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Point.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>

class PointsDifferenceOperator: public GenericOperator {
public:
	PointsDifferenceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
	virtual ~PointsDifferenceOperator();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
private:
	double epsilonDistance;
};

PointsDifferenceOperator::PointsDifferenceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);

	epsilonDistance = params.get("epsilonDistance", 0).asDouble();
}

PointsDifferenceOperator::~PointsDifferenceOperator() {}
REGISTER_OPERATOR(PointsDifferenceOperator, "points_difference");

std::unique_ptr<PointCollection> PointsDifferenceOperator::getPoints(const QueryRectangle &rect) {
	auto pointsMinuend = getPointsFromSource(0, rect);
	auto pointsSubtrahend = getPointsFromSource(1, rect);

	auto pointsOut = std::make_unique<PointCollection>(pointsMinuend->epsg);

	PointCollectionMetadataCopier metadataCopier(*pointsMinuend, *pointsOut);
	metadataCopier.copyGlobalMetadata();
	metadataCopier.initLocalMetadataFields();

	pointsOut->lock();

	if(std::fabs(epsilonDistance) < std::numeric_limits<double>::epsilon()) {
		// use faster method for equality

		// comparator that compares first to x1 < x2 and then to y1 < y2
		auto compareFunction = [](const Point& p1, const Point& p2) -> bool {
			return (p1.x < p2.x) || ((std::fabs(p1.x - p2.x) < std::numeric_limits<double>::epsilon()) && (p1.y < p2.y));
		};

		std::sort(pointsMinuend->collection.begin(), pointsMinuend->collection.end(), compareFunction);
		std::sort(pointsSubtrahend->collection.begin(), pointsSubtrahend->collection.end(), compareFunction);

		std::set_difference(pointsMinuend->collection.begin(), pointsMinuend->collection.end(),
							pointsSubtrahend->collection.begin(), pointsSubtrahend->collection.end(),
							std::inserter(pointsOut->collection, pointsOut->collection.begin()),
							compareFunction);

	} else {
		// use geoms

		// create factory for geos points
		const geos::geom::PrecisionModel pm;
		geos::geom::GeometryFactory geometryFactory = geos::geom::GeometryFactory(&pm, pointsMinuend->epsg);
		auto geomDeleter = [&](geos::geom::Point* pointGeom) {
			geometryFactory.destroyGeometry(pointGeom);
		};

		for (Point& pointMinuend : pointsMinuend->collection) {
			std::unique_ptr<geos::geom::Point, decltype(geomDeleter)> pointGeomMinuend(
					geometryFactory.createPoint(geos::geom::Coordinate(pointMinuend.x, pointMinuend.y)),
					geomDeleter);

			bool isSubtracted = false;
			for (Point& pointSubtrahend : pointsSubtrahend->collection) {
				std::unique_ptr<geos::geom::Point, decltype(geomDeleter)> pointGeomSubtrahend(
						geometryFactory.createPoint(geos::geom::Coordinate(pointSubtrahend.x, pointSubtrahend.y)),
						geomDeleter);

				if(pointGeomMinuend->isWithinDistance(pointGeomSubtrahend.get(), epsilonDistance)) {
					isSubtracted = true;
					break;
				}
			}

			if(!isSubtracted) {
				pointsOut->addPoint(pointMinuend.x, pointMinuend.y);
			}
		}

	}

	return pointsOut;
}
