#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "operators/operator.h"

#include "util/make_unique.h"

#include <string>
#include <sstream>
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"

/**
 * Filter simple pointcollection by a polygoncollection
 */
class FilterPointsByGeometry : public GenericOperator {
	public:
		FilterPointsByGeometry(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~FilterPointsByGeometry();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
};




FilterPointsByGeometry::FilterPointsByGeometry(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);
}

FilterPointsByGeometry::~FilterPointsByGeometry() {
}
REGISTER_OPERATOR(FilterPointsByGeometry, "filterpointsbygeometry");


#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<PointCollection> FilterPointsByGeometry::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointCollectionFromSource(0, rect, profiler, FeatureCollectionQM::SINGLE_ELEMENT_FEATURES);
	auto multiPolygons = getPolygonCollectionFromSource(0, rect, profiler, FeatureCollectionQM::ANY_FEATURE);

	size_t points_count = points->getFeatureCount();
	std::vector<bool> keep(points_count, false);

	//TODO: more efficient batch processing? (http://alienryderflex.com/polygon/)
	for(auto feature : *points){
		for(auto& coordinate : feature){
			if(multiPolygons->pointInCollection(coordinate)){
				keep[feature] = true;
				break;
			}
		}
	}

	return points->filter(keep);
}
#endif

