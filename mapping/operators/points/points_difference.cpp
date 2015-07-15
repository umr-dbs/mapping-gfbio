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
#include "datatypes/pointcollection.h"

class PointsDifferenceOperator: public GenericOperator {
	public:
		PointsDifferenceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PointsDifferenceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
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

#ifndef MAPPING_OPERATOR_STUBS
#include "operators/points/points_difference.cl.h"

static double point_distance(const Coordinate &p1, const Coordinate &p2) {
	double dx = p1.x - p2.x, dy = p1.y - p2.y;
	return sqrt(dx*dx + dy*dy);
}
//TODO: migrate to new multi semantics
std::unique_ptr<PointCollection> PointsDifferenceOperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto pointsMinuend = getPointCollectionFromSource(0, rect, profiler);
	auto pointsSubtrahend = getPointCollectionFromSource(1, rect, profiler);

	//fprintf(stderr, "Minuend: %lu, Subtrahend: %lu\n", pointsMinuend->collection.size(), pointsSubtrahend->collection.size());

	size_t count_m = pointsMinuend->coordinates.size();

	// TODO: why is there a limitation? remove or make more reasonable abort decisions.
	if (count_m > 100000)
		throw OperatorException("Too many points for points_difference, aborting");


#ifdef MAPPING_NO_OPENCL
	std::vector<bool> keep(count_m, true);

	size_t count_s = pointsSubtrahend->coordinates.size();

	for (size_t idx_m=0;idx_m<count_m;idx_m++) {
		Coordinate &p_m = pointsMinuend->coordinates[idx_m];

		for (size_t idx_s=0;idx_s<count_s;idx_s++) {
			if (point_distance(p_m, pointsSubtrahend->coordinates[idx_s]) <= epsilonDistance) {
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
#endif
