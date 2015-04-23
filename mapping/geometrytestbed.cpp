#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <vector>

#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "raster/opencl.h"
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
	geos::geom::Geometry* geometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))), MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");

	auto multiPolygonCollection = GeosGeomUtil::createPolygonCollection(*geometry);

	std::cout << "points" << std::endl;
	for(auto p = multiPolygonCollection->coordinates.begin(); p != multiPolygonCollection->coordinates.end(); ++p){
	    std::cout << (*p).x << "," << (*p).y << ' ';
	}

	std::cout << std::endl;
	std::cout << "rings" << std::endl;
	for(auto p = multiPolygonCollection->start_ring.begin(); p != multiPolygonCollection->start_ring.end(); ++p){
		    std::cout << *p << ' ';
	}

	std::cout << std::endl;
	std::cout << "polygons" << std::endl;
	for(auto p = multiPolygonCollection->start_polygon.begin(); p != multiPolygonCollection->start_polygon.end(); ++p){
		    std::cout << *p << ' ';
	}

	std::cout << std::endl;
	std::cout << "features" << std::endl;
	for(auto p = multiPolygonCollection->start_feature.begin(); p != multiPolygonCollection->start_feature.end(); ++p){
		    std::cout << *p << ' ';
	}

	std::cout << multiPolygonCollection->toGeoJSON(false);
}

void testTwoWay(){
	geos::geom::Geometry* geometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");

	auto multiPolygonCollection = GeosGeomUtil::createPolygonCollection(*geometry);

	auto geos = GeosGeomUtil::createGeosGeometry(*multiPolygonCollection);

	std::cout << multiPolygonCollection->toGeoJSON(false);
}

void testMultiPointToCSV(){
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.local_md_value.set(1, "test", 2.1);

	points.finishFeature();

	std::cout << points.toCSV();
}


void testMultiPointGeoJSONWithMetadata(){
	PointCollection points(SpatioTemporalReference::unreferenced());

	points.local_md_value.addVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);

	std::cout << points.toGeoJSON(true);
}

void testMultiPolygonGeoJSON(){
	PolygonCollection multiPolygonCollection(SpatioTemporalReference::unreferenced());

	multiPolygonCollection.addCoordinate(1, 2);
	multiPolygonCollection.addCoordinate(2, 3);
	multiPolygonCollection.addCoordinate(1, 2);
	multiPolygonCollection.finishRing();
	multiPolygonCollection.finishPolygon();
	multiPolygonCollection.finishFeature();

	for(auto x = multiPolygonCollection.coordinates.begin(); x != multiPolygonCollection.coordinates.end(); ++x){
		std::cout << (*x).x;
	}
	std::cout << std::endl;
	for(auto x = multiPolygonCollection.start_ring.begin(); x != multiPolygonCollection.start_ring.end(); ++x){
		std::cout << *x;
	}
	std::cout << std::endl;
	for(auto x = multiPolygonCollection.start_polygon.begin(); x != multiPolygonCollection.start_polygon.end(); ++x){
		std::cout << *x;
	}
	std::cout << std::endl;
	for(auto x = multiPolygonCollection.start_feature.begin(); x != multiPolygonCollection.start_feature.end(); ++x){
		std::cout << *x;
	}
	std::cout << std::endl;


	std::cout << multiPolygonCollection.toGeoJSON(false);
}

void testFilterPoints(){
	PointCollection multiPointCollection(SpatioTemporalReference::unreferenced());
	multiPointCollection.addSinglePointFeature(Coordinate(1,2));
	multiPointCollection.addSinglePointFeature(Coordinate(3,4));

	std::cout << multiPointCollection.getAsString();

	std::vector<bool> keep;
	keep.push_back(true);
	keep.push_back(false);

	auto m = multiPointCollection.filter(keep);

	std::cout << m->toGeoJSON(false);
}

int main(){
//	std::cout << "hello" << std::endl;
//
//	std::cout << "hello again" << std::endl;
//
//
//testGeosToMapping();
//	testTwoWay();

	//testMultiPolygonGeoJSON();
	//testFilterPoints();

	//testMultiPointToCSV();
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());

	points.local_md_value.addVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);

	std::cout << points.toGeoJSON(true);
	return 0;
}
