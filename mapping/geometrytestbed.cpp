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

#include "operators/operator.h"
#include <boost/tokenizer.hpp>

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

	auto multiPolygonCollection = GeosGeomUtil::createPolygonCollection(*geometry, SpatioTemporalReference::unreferenced());

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

	auto multiPolygonCollection = GeosGeomUtil::createPolygonCollection(*geometry, SpatioTemporalReference::unreferenced());

	auto geos = GeosGeomUtil::createGeosPolygonCollection(*multiPolygonCollection);

	std::cout << multiPolygonCollection->toGeoJSON(false);
}

void testMultiPointToCSV(){
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addEmptyVector("test");

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

	points.local_md_value.addEmptyVector("test");

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

double iteratorBenchmarksStraight(const SimpleFeatureCollection &sfc) {
	double res = 0;
	auto size = sfc.coordinates.size();
	for (size_t i=0;i<size;i++)
		res += sfc.coordinates[i].x;
	return res;
}
double iteratorBenchmarksStraightForeach(const PointCollection &pc) {
	double res = 0;
	for (auto && c : pc.coordinates)
		res += c.x;
	return res;
}
double iteratorBenchmarksLoops(const PointCollection &pc) {
	double res = 0;
	auto featurecount = pc.getFeatureCount();
	for (size_t i=0;i<featurecount;i++) {
		auto start = pc.start_feature[i];
		auto end = pc.start_feature[i+1];
		for (size_t j = start; j < end; j++) {
			res += pc.coordinates[j].x;
		}
	}
	return res;
}
double iteratorBenchmarksIterators(const PointCollection &pc) {
	double res = 0;
	for (auto feature : pc) {
		for (auto & c : feature) {
			res += c.x;
		}
	}
	return res;
}
double iteratorBenchmarksModify(PointCollection &pc) {
	double res = 0;

	auto & vec = pc.local_md_value.getVector("Value");
	double values = 0;
	for (auto feature : pc) {
		for (auto & c : feature) {
			c.x += 1;
			res += c.x;
			values += vec[feature];
		}
	}
	return res + values;
}

void iteratorBenchmarksPoints(PointCollection &pc, int iter) {
	auto t1 = clock();
	auto res1 = iteratorBenchmarksStraight(pc);
	auto t2 = clock();
	auto res2 = iteratorBenchmarksStraightForeach(pc);
	auto t3 = clock();
	auto res3 = iteratorBenchmarksLoops(pc);
	auto t4 = clock();
	auto res4 = iteratorBenchmarksIterators(pc);
	auto t5 = clock();
	auto res5 = iteratorBenchmarksIterators(pc);
	auto t6 = clock();
	auto res6 = iteratorBenchmarksModify(pc);
	auto t7 = clock();

	printf("Iteration %d:\nStraight: %7.3fms (%f)\nForeach:  %7.3fms (%f)\nLoops:    %7.3fms (%f)\nIterator: %7.3fms (%f)\nIterator: %7.3fms (%f)\nModify:   %7.3fms (%f)\n", iter,
		1000.0*(t2-t1)/CLOCKS_PER_SEC, res1,
		1000.0*(t3-t2)/CLOCKS_PER_SEC, res2,
		1000.0*(t4-t3)/CLOCKS_PER_SEC, res3,
		1000.0*(t5-t4)/CLOCKS_PER_SEC, res4,
		1000.0*(t6-t5)/CLOCKS_PER_SEC, res5,
		1000.0*(t7-t6)/CLOCKS_PER_SEC, res6
	);
}


double iteratorBenchmarksPolyLoops(const PolygonCollection &pc) {
	double res = 0;
	auto featurecount = pc.getFeatureCount();
	for (size_t i=0;i<featurecount;i++) {
		auto startf = pc.start_feature[i];
		auto endf = pc.start_feature[i+1];
		for (size_t f = startf; f < endf; f++) {
			auto startp = pc.start_polygon[f];
			auto endp = pc.start_polygon[f+1];
			for (size_t p = startp;p < endp; p++) {
				auto startr = pc.start_ring[p];
				auto endr = pc.start_ring[p+1];
				for (size_t r = startr; r < endr; r++) {
					res += pc.coordinates[r].x;
				}
			}
		}
	}
	return res;
}
double iteratorBenchmarksPolyIterators(const PolygonCollection &pc) {
	double res = 0;
	for (auto feature: pc)
		for (auto polygon : feature)
			for (auto ring : polygon)
				for (auto & c : ring) {
					res += c.x;
				}

	return res;
}
double iteratorBenchmarksPolyModify(PolygonCollection &pc) {
	double res = 0;
	for (auto feature: pc) {
		for (auto polygon : feature)
			for (auto ring : polygon)
				for (auto & c : ring) {
					c.x += 1;
					res += c.x;
				}
	}
	return res;
}

void iteratorBenchmarksPoly(PolygonCollection &pc, int iter) {
	double res1 = 0, res2 = 0, res3 = 0, res4 = 0;
	auto t1 = clock();
	for (int i=0;i<1000;i++)
		res1 += iteratorBenchmarksStraight(pc);
	auto t2 = clock();
	for (int i=0;i<1000;i++)
		res2 += iteratorBenchmarksPolyLoops(pc);
	auto t3 = clock();
	for (int i=0;i<1000;i++)
		res3 += iteratorBenchmarksPolyIterators(pc);
	auto t4 = clock();
	for (int i=0;i<1000;i++)
		res4 += iteratorBenchmarksPolyModify(pc);
	auto t5 = clock();

	printf("Iteration %d:\nStraight: %7.3fms (%f)\nLoops:    %7.3fms (%f)\nIterator: %7.3fms (%f)\nModify:   %7.3fms (%f)\n", iter,
		1000.0*(t2-t1)/CLOCKS_PER_SEC, res1,
		1000.0*(t3-t2)/CLOCKS_PER_SEC, res2,
		1000.0*(t4-t3)/CLOCKS_PER_SEC, res3,
		1000.0*(t5-t4)/CLOCKS_PER_SEC, res4
	);
}

void iteratorBenchmarks() {

	auto op = GenericOperator::fromJSON("{\"type\": \"csvpointsource\", \"params\":{ \"filename\": \"/home/rastersources/safecast/onemillion.csv\" } }");
	QueryRectangle qrect(
		SpatialReference(EPSG_LATLON, -180, -90, 180, 90),
		TemporalReference(TIMETYPE_UNIX, 42, 42),
		QueryResolution::none()
	);
	QueryProfiler profiler;
	printf("Loading point data...\n");
	auto pc = op->getCachedPointCollection(qrect, profiler);
	printf("Working on PC with %lu features\n", pc->getFeatureCount());
	iteratorBenchmarksPoints(*pc, 1);
	iteratorBenchmarksPoints(*pc, 2);
	iteratorBenchmarksPoints(*pc, 3);

	printf("Loading polygon data...\n");
	geos::geom::Geometry* geometry = createGeosGeometry("GEOMETRYCOLLECTION(MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))), MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))))");
	auto poly = GeosGeomUtil::createPolygonCollection(*geometry, SpatioTemporalReference::unreferenced());

	iteratorBenchmarksPoly(*poly, 1);
	iteratorBenchmarksPoly(*poly, 2);
	iteratorBenchmarksPoly(*poly, 3);
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

//	iteratorBenchmarks();
//
//	//testMultiPointToCSV();
//	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
//
//	points.local_md_value.addEmptyVector("test");
//
//	points.addCoordinate(1,2);
//	points.finishFeature();
//	points.local_md_value.set(0, "test", 5.1);
//
//	points.addCoordinate(2,3);
//	points.addCoordinate(3,4);
//	points.finishFeature();
//	points.local_md_value.set(1, "test", 2.1);
//
//	std::cout << points.toGeoJSON(true);

	std::string string = "this thing\nis. not\na\ntest";
	boost::char_delimiters_separator<char> lineSeparator(false, "", "\n");

	boost::tokenizer<boost::char_delimiters_separator<char>> lineTokenizer(string, lineSeparator);

	for(auto lineToken = lineTokenizer.begin(); lineToken != lineTokenizer.end(); ++lineToken){
		std::cout << ">" << *lineToken <<  "<" << std::endl;
	}

	return 0;
}
