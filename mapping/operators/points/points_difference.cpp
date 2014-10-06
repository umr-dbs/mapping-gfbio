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

static double point_distance(const Point &p1, const Point &p2) {
	double dx = p1.x - p2.x, dy = p1.y - p2.y;
	return sqrt(dx*dx + dy*dy);
}

std::unique_ptr<PointCollection> PointsDifferenceOperator::getPoints(const QueryRectangle &rect) {
	auto pointsMinuend = getPointsFromSource(0, rect);
	auto pointsSubtrahend = getPointsFromSource(1, rect);

	size_t count_m = pointsMinuend->collection.size();
	std::vector<bool> keep(count_m, true);

	size_t count_s = pointsSubtrahend->collection.size();

	for (size_t idx_m=0;idx_m<count_m;idx_m++) {
		Point &p_m = pointsMinuend->collection[idx_m];

		for (size_t idx_s=0;idx_s<count_s;idx_s++) {
			if (point_distance(p_m, pointsSubtrahend->collection[idx_s]) < epsilonDistance) {
				keep[idx_m] = false;
				break;
			}
		}
	}

	return pointsMinuend->filter(keep);
}
