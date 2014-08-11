#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <algorithm>
#include <functional>

class PointsDifferenceOperator: public GenericOperator {
public:
	PointsDifferenceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
	virtual ~PointsDifferenceOperator();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};

PointsDifferenceOperator::PointsDifferenceOperator(int sourcecount,	GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(2);
}

PointsDifferenceOperator::~PointsDifferenceOperator() {
}
REGISTER_OPERATOR(PointsDifferenceOperator, "points_difference");

std::unique_ptr<PointCollection> PointsDifferenceOperator::getPoints(const QueryRectangle &rect) {
	auto pointsMinuend = sources[0]->getPoints(rect);
	auto pointsSubtrahend = sources[1]->getPoints(rect);

	auto pointsOut = std::make_unique<PointCollection>(pointsMinuend->epsg);

	PointCollectionMetadataCopier metadataCopier(*pointsMinuend, *pointsOut);
	metadataCopier.copyGlobalMetadata();
	metadataCopier.initLocalMetadataFields();

	// insert and remove fake point to LOCK collection
	pointsOut->addPoint(0, 0);
	pointsOut->collection.clear();

	// comparator that compares first to x1 < x2 and then to y1 < y2
	std::function<bool(const Point&, const Point&)> compareFunction = [](const Point& p1, const Point& p2) -> bool {
		return (p1.x < p2.x) || ((std::fabs(p1.x - p2.x) < std::numeric_limits<double>::epsilon()) && (p1.y < p2.y));
	};

	std::sort(pointsMinuend->collection.begin(), pointsMinuend->collection.end(), compareFunction);
	std::sort(pointsSubtrahend->collection.begin(), pointsSubtrahend->collection.end(), compareFunction);

	std::set_difference(pointsMinuend->collection.begin(), pointsMinuend->collection.end(),
						pointsSubtrahend->collection.begin(), pointsSubtrahend->collection.end(),
						std::inserter(pointsOut->collection, pointsOut->collection.begin()),
						compareFunction);

	return pointsOut;
}
