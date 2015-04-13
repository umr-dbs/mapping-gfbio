#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/io/WKBReader.h>
#include "wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <sstream>
#include "util/make_unique.h"
#include "raster/exceptions.h"


WKBUtil::~WKBUtil() {

}

//read Multipolygon as we currently need it for GFBioWS
//TODO: support Polygons, MultiPolygons, Collection of Polygons, Collection of MultiPolygon, Collection of mixed (Multi-)Polygons
std::unique_ptr<MultiPolygonCollection> WKBUtil::readMultiPolygonCollection(std::stringstream& wkb){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	wkb.seekg(0);
	geos::geom::Geometry* geom = wkbreader.read(wkb);
	std::cerr << geom->getGeometryTypeId() << geom->getGeometryN(0)->getGeometryType();
	if(geom->getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	if(geom->getNumGeometries() == 0 || geom->getGeometryN(0)->getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON){
			throw ConverterException("GEOS Geometry is not a geometry MultiPolygon or does not exist");
	}

	const geos::geom::MultiPolygon* multiPolygon = dynamic_cast<const geos::geom::MultiPolygon*>(geom->getGeometryN(0));

	auto multiPolygonCollection = GeosGeomUtil::createMultiPolygonCollection(*multiPolygon);

	gf->destroyGeometry(geom);

	std::cerr << "OK";

	return multiPolygonCollection;
}
