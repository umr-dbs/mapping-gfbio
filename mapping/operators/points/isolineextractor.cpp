#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include <json/json.h>
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <geos/geom/GeometryFactory.h>


/**
 * Operator that cextracts isolines from a given point collection
 */
class IsolineExtractor : public GenericOperator {
	public:
	IsolineExtractor(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(1);
		}

		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			auto points = getPointCollectionFromSource(0, rect, profiler, FeatureCollectionQM::SINGLE_ELEMENT_FEATURES);

			auto geom = GeosGeomUtil::createGeosPointCollection(*points);

			auto *gf = geos::geom::GeometryFactory::getDefaultInstance();

			std::vector<geos::geom::Geometry*> geosLines;
			while(geom->getNumGeometries() > 2){
				auto hull = geom->convexHull()->getBoundary();
				geosLines.push_back(hull);

				geom.reset(geom->difference(gf->createMultiPoint(*hull->getCoordinates())));
			}

			auto collection = gf->createGeometryCollection(geosLines);

			auto lines = GeosGeomUtil::createLineCollection(*collection);

			gf->destroyGeometry(collection);

			return lines;
		}

		virtual ~IsolineExtractor(){};

	private:
		std::string wkt;
		std::string type;
};
REGISTER_OPERATOR(IsolineExtractor, "isolineextractor");
