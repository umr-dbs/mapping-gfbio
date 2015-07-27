
#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "operators/operator.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"

#include "util/make_unique.h"

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>

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
	//TODO: check projection
	const geos::geom::PrecisionModel pm;
	geos::geom::GeometryFactory gf = geos::geom::GeometryFactory(&pm, 4326);
	geos::geom::GeometryFactory* geometryFactory = &gf;

	auto points = getPointCollectionFromSource(0, rect, profiler, FeatureCollectionQM::SINGLE_ELEMENT_FEATURES);

	auto multiPolygons = getPolygonCollectionFromSource(0, rect, profiler, FeatureCollectionQM::ANY_FEATURE);


	size_t points_count = points->getFeatureCount();
	std::vector<bool> keep(points_count, false);

	auto prep = geos::geom::prep::PreparedGeometryFactory();

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

