#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKTReader.h>
#include <geos/util/IllegalArgumentException.h>
#include "wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <sstream>
#include "util/make_unique.h"
#include "util/exceptions.h"
#include <memory>


std::unique_ptr<PointCollection> WKBUtil::readPointCollection(std::stringstream& wkb, const SpatioTemporalReference& stref){
	//TODO: implement
	throw FeatureException("Read not yet implemented");
}

std::unique_ptr<LineCollection> WKBUtil::readLineCollection(std::stringstream& wkb, const SpatioTemporalReference& stref){
	//TODO: implement
	throw FeatureException("Read not yet implemented");
}

std::unique_ptr<PolygonCollection> WKBUtil::readPolygonCollection(std::stringstream& wkb, const SpatioTemporalReference& stref){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKBReader wkbreader(*gf);

	wkb.seekg(0);
	geos::geom::Geometry* geom = wkbreader.read(wkb);
	std::cerr << geom->getGeometryTypeId() << geom->getGeometryN(0)->getGeometryType();
	if(geom->getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}


	auto polygonCollection = GeosGeomUtil::createPolygonCollection(*geom, stref);

	gf->destroyGeometry(geom);

	return polygonCollection;
}

std::unique_ptr<PointCollection> WKBUtil::readPointCollection(const std::string& wkt, const SpatioTemporalReference& stref){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto pointCollection = GeosGeomUtil::createPointCollection(*geom, stref);

	gf->destroyGeometry(geom);

	return pointCollection;
}

std::unique_ptr<LineCollection> WKBUtil::readLineCollection(const std::string& wkt, const SpatioTemporalReference& stref){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto lineCollection = GeosGeomUtil::createLineCollection(*geom, stref);

	gf->destroyGeometry(geom);

	return lineCollection;
}

std::unique_ptr<PolygonCollection> WKBUtil::readPolygonCollection(const std::string& wkt, const SpatioTemporalReference& stref){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	geos::geom::Geometry* geom = wktreader.read(wkt);

	auto polygonCollection = GeosGeomUtil::createPolygonCollection(*geom, stref);

	gf->destroyGeometry(geom);

	return polygonCollection;
}

struct GeometryDeleter {
	void operator()(geos::geom::Geometry* geom) const {
		geom->getFactory()->destroyGeometry(geom);
	}
};

void WKBUtil::addFeatureToCollection(PointCollection& collection, const std::string& wkt){
	size_t coordinates = collection.coordinates.size();
	size_t features = collection.start_feature.size();

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	std::unique_ptr<geos::geom::Geometry, GeometryDeleter> geom(wktreader.read(wkt));

	try {
		GeosGeomUtil::addFeatureToCollection(collection, *geom);
	} catch(const FeatureException& e) {
		if(collection.coordinates.size() != coordinates || collection.start_feature.size() != features){
			collection.removeLastFeature();
		}
		throw;
	}
}

void WKBUtil::addFeatureToCollection(LineCollection& collection, const std::string& wkt){
	size_t coordinates = collection.coordinates.size();
	size_t lines = collection.start_line.size();
	size_t features = collection.start_feature.size();
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	std::unique_ptr<geos::geom::Geometry, GeometryDeleter> geom(wktreader.read(wkt));

	try {
		GeosGeomUtil::addFeatureToCollection(collection, *geom);
	} catch(const FeatureException& e) {
		if(collection.coordinates.size() != coordinates || collection.start_line.size() != lines || collection.start_feature.size() != features){
			collection.removeLastFeature();
		}
		throw;
	}
}

void WKBUtil::addFeatureToCollection(PolygonCollection& collection, const std::string& wkt){
	size_t coordinates = collection.coordinates.size();
	size_t rings = collection.start_ring.size();
	size_t polygons = collection.start_polygon.size();
	size_t features = collection.start_feature.size();
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);

	std::unique_ptr<geos::geom::Geometry, GeometryDeleter> geom(wktreader.read(wkt));

	try {
		GeosGeomUtil::addFeatureToCollection(collection, *geom);
	} catch(const FeatureException& e) {
		if(collection.coordinates.size() != coordinates || collection.start_ring.size() != rings || collection.start_polygon.size() != polygons || collection.start_feature.size() != features){
			collection.removeLastFeature();
		}
		throw;
	}
}
