
#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/pointcollection.h"
#include "raster/geometry.h"
#include "operators/operator.h"

#include "util/make_unique.h"

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>

#include <string>
#include <sstream>

class FilterPointsByGeometry : public GenericOperator {
	public:
		FilterPointsByGeometry(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~FilterPointsByGeometry();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};




FilterPointsByGeometry::FilterPointsByGeometry(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(2);
}

FilterPointsByGeometry::~FilterPointsByGeometry() {
}
REGISTER_OPERATOR(FilterPointsByGeometry, "filterpointsbygeometry");


std::unique_ptr<PointCollection> FilterPointsByGeometry::getPoints(const QueryRectangle &rect) {
	//TODO: check projection
	const geos::geom::PrecisionModel pm;
	geos::geom::GeometryFactory gf = geos::geom::GeometryFactory(&pm, 4326);
	geos::geom::GeometryFactory* geometryFactory = &gf;

	auto points = sources[0]->getPoints(rect);

	auto generic_geometry = sources[1]->getGeometry(rect);
	auto geometry = generic_geometry->getGeometry();
	fprintf(stderr,"getGeom >> %f", geometry->getArea());

	auto points_out = std::make_unique<PointCollection>(rect.epsg);

	auto prep = geos::geom::prep::PreparedGeometryFactory();

	for (int i=0; i<geometry->getNumGeometries(); i++){

		auto preparedGeometry = prep.prepare(geometry->getGeometryN(i));
		for (Point &p : points->collection) {
			double x = p.x, y = p.y;

			const geos::geom::Coordinate coordinate(x, y);
			geos::geom::Point* pointGeom = geometryFactory->createPoint(coordinate);

			if(preparedGeometry->contains(pointGeom)){
				//TODO copy metadata
				points_out->addPoint(x, y);
				geometryFactory->destroyGeometry(pointGeom);
				continue;
				//TODO: dont check this point again
			}

			geometryFactory->destroyGeometry(pointGeom);
		}

		prep.destroy(preparedGeometry);
	}

	return points_out;
}
