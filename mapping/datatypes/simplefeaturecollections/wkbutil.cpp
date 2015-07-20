#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKTReader.h>
#include "wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <sstream>
#include "util/make_unique.h"
#include "util/exceptions.h"


std::unique_ptr<PointCollection> WKBUtil::readPointCollection(std::stringstream& wkb){
	//TODO: implement
	throw FeatureException("Read not yet implemented");
}

std::unique_ptr<LineCollection> WKBUtil::readLineCollection(std::stringstream& wkb){
	//TODO: implement
	throw FeatureException("Read not yet implemented");
}

std::unique_ptr<PolygonCollection> WKBUtil::readPolygonCollection(std::stringstream& wkb){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	wkb.seekg(0);
	geos::geom::Geometry* geom = wkbreader.read(wkb);
	std::cerr << geom->getGeometryTypeId() << geom->getGeometryN(0)->getGeometryType();
	if(geom->getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}


	auto polygonCollection = GeosGeomUtil::createPolygonCollection(*geom);

	gf->destroyGeometry(geom);

	return polygonCollection;
}

std::unique_ptr<PointCollection> WKBUtil::readPointCollection(std::string& wkt){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto pointCollection = GeosGeomUtil::createPointCollection(*geom);

	gf->destroyGeometry(geom);

	return pointCollection;
}

std::unique_ptr<LineCollection> WKBUtil::readLineCollection(std::string& wkt){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto lineCollection = GeosGeomUtil::createLineCollection(*geom);

	gf->destroyGeometry(geom);

	return lineCollection;
}

std::unique_ptr<PolygonCollection> WKBUtil::readPolygonCollection(std::string& wkt){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto polygonCollection = GeosGeomUtil::createPolygonCollection(*geom);

	gf->destroyGeometry(geom);

	return polygonCollection;
}
