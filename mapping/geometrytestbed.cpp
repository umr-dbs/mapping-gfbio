#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include "datatypes/multipolygoncollection.h"
#include "datatypes/multipointcollection.h"
#include <vector>

geos::geom::Geometry* createGeosGeometry(std::string wkt){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();
	geos::io::WKTReader wktreader(*gf);
	return wktreader.read(wkt);
}

//Test input of collection containing a single multipolygon as gfbioWS currently outputs
void testGFBioInput(){
	geos::geom::Geometry* regularGeometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");



}


void testGeosToMapping(){
	geos::geom::Geometry* geometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");

	auto multiPolygonCollection = GeosGeomUtil::createMultiPolygonCollection(*geometry);

	std::cout << "points" << std::endl;
	for(auto p = multiPolygonCollection->coordinates.begin(); p != multiPolygonCollection->coordinates.end(); ++p){
	    std::cout << (*p).x << "," << (*p).y << ' ';
	}

	std::cout << std::endl;
	std::cout << "rings" << std::endl;
	for(auto p = multiPolygonCollection->startRing.begin(); p != multiPolygonCollection->startRing.end(); ++p){
		    std::cout << *p << ' ';
	}

	std::cout << std::endl;
	std::cout << "polygons" << std::endl;
	for(auto p = multiPolygonCollection->startPolygon.begin(); p != multiPolygonCollection->startPolygon.end(); ++p){
		    std::cout << *p << ' ';
	}

	std::cout << std::endl;
	std::cout << "features" << std::endl;
	for(auto p = multiPolygonCollection->startFeature.begin(); p != multiPolygonCollection->startFeature.end(); ++p){
		    std::cout << *p << ' ';
	}
}

void testTwoWay(){
	geos::geom::Geometry* geometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");

	auto multiPolygonCollection = GeosGeomUtil::createMultiPolygonCollection(*geometry);

	auto geos = GeosGeomUtil::createGeosGeometry(*multiPolygonCollection);

	std::cout << multiPolygonCollection->toGeoJSON(false);
}

void testMultiPointToCSV(){
	MultiPointCollection multiPointCollection(SpatioTemporalReference::unreferenced());

	std::vector<Coordinate> coordinates;
	coordinates.push_back(Coordinate(1,2));

	multiPointCollection.addFeature(coordinates);

	coordinates.clear();
	coordinates.push_back(Coordinate(1,2));
	coordinates.push_back(Coordinate(2,3));

	multiPointCollection.addFeature(coordinates);

	std::cout << multiPointCollection.toCSV();
}

int main(){
//	std::cout << "hello" << std::endl;
//
//	std::cout << "hello again" << std::endl;
//
//
//	//testGeosToMapping();
//	testTwoWay();

	testMultiPointToCSV();

	return 0;
}
