#include "datatypes/pointcollection.h"
#include "raster/opencl.h"
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

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect, QueryProfiler &profiler);

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		double epsilonDistance;
};

PointsDifferenceOperator::PointsDifferenceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);

	epsilonDistance = params.get("epsilonDistance", 0).asDouble();
}

PointsDifferenceOperator::~PointsDifferenceOperator() {}
REGISTER_OPERATOR(PointsDifferenceOperator, "points_difference");

void PointsDifferenceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"epsilonDistance\":" << epsilonDistance;
}

#include "operators/points/points_difference.cl.h"

static double point_distance(const Point &p1, const Point &p2) {
	double dx = p1.x - p2.x, dy = p1.y - p2.y;
	return sqrt(dx*dx + dy*dy);
}

std::unique_ptr<PointCollection> PointsDifferenceOperator::getPoints(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto pointsMinuend = getPointsFromSource(0, rect, profiler);
	auto pointsSubtrahend = getPointsFromSource(1, rect, profiler);

	//fprintf(stderr, "Minuend: %lu, Subtrahend: %lu\n", pointsMinuend->collection.size(), pointsSubtrahend->collection.size());

	size_t count_m = pointsMinuend->collection.size();

	// TODO: why is there a limitation? remove or make more reasonable abort decisions.
	if (count_m > 100000)
		throw OperatorException("Too many points for points_difference, aborting");


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
		prog.setProfiler(profiler);
		prog.addPointCollection(pointsMinuend.get());
		prog.addPointCollection(pointsSubtrahend.get());
		prog.compile(operators_points_points_difference, "difference");
		prog.addPointCollectionPositions(0, true);
		prog.addPointCollectionPositions(1, true);
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
