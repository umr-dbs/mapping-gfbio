#include "raster/pointcollection.h"
#include "raster/opencl.h"
#include "raster/profiler.h"
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


#include "operators/points/points_difference.cl.h"

static double point_distance(const Point &p1, const Point &p2) {
	double dx = p1.x - p2.x, dy = p1.y - p2.y;
	return sqrt(dx*dx + dy*dy);
}

std::unique_ptr<PointCollection> PointsDifferenceOperator::getPoints(const QueryRectangle &rect) {
	auto pointsMinuend = getPointsFromSource(0, rect);
	auto pointsSubtrahend = getPointsFromSource(1, rect);

	Profiler::Profiler p("POINTS_DIFFERENCE_OPERATOR");

	size_t count_m = pointsMinuend->collection.size();
#ifdef MAPPING_NO_OPENCL
	std::vector<bool> keep(count_m, true);

	size_t count_s = pointsSubtrahend->collection.size();

	for (size_t idx_m=0;idx_m<count_m;idx_m++) {
		Point &p_m = pointsMinuend->collection[idx_m];

		for (size_t idx_s=0;idx_s<count_s;idx_s++) {
			if (point_distance(p_m, pointsSubtrahend->collection[idx_s]) <= epsilonDistance) {
				keep[idx_m] = false;
				break;
			}
		}
	}
#else
	RasterOpenCL::init();

	std::vector<char> keep(count_m, true);

	try {
		RasterOpenCL::CLProgram prog;
		prog.addPointCollection(pointsMinuend.get());
		prog.addPointCollection(pointsSubtrahend.get());
		prog.compile(operators_points_points_difference, "difference");
		prog.addPointCollectionPositions(0);
		prog.addPointCollectionPositions(1);
		prog.addArg(keep);
		prog.addArg(epsilonDistance);
		prog.run();
	}
	catch (cl::Error &e) {
		printf("cl::Error %d: %s\n", e.err(), e.what());
		throw;
	}

#endif
	return pointsMinuend->filter(keep);
}
