#include <gtest/gtest.h>
#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include "util/binarystream.h"

#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "raster/opencl.h"
#include "test/unittests/simplefeaturecollections/util.h"

std::unique_ptr<PolygonCollection> createPolygonsWithAttributesAndTime(){
	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((10 10, 10 30, 25 20, 10 10)), POLYGON((15 70, 25 90, 45 90, 40 80, 50 70, 15 70), (30 75, 25 80, 30 85, 35 80, 30 75)), POLYGON((50 30, 65 60, 100 25, 50 30), (55 35, 65 45, 65 35, 55 35), (75 30, 75 35, 85 35, 85 30, 75 30)), MULTIPOLYGON(((15 50, 15 60, 30 65, 35 60 25 50, 15 50)), ((30 35, 35 45, 40 34, 30 35))))";
	auto polygons = WKBUtil::readPolygonCollection(wkt, SpatioTemporalReference::unreferenced());
	polygons->time_start = {2, 4,  8, 16};
	polygons->time_end =   {4, 8, 16, 32};

	polygons->global_attributes.setTextual("info", "1234");
	polygons->global_attributes.setNumeric("index", 42);

	polygons->feature_attributes.addNumericAttribute("value", Unit::unknown(), {0.0, 1.1, 2.2, 3.3});
	polygons->feature_attributes.addTextualAttribute("label", Unit::unknown(), {"l0", "l1", "l2", "l3"});

	EXPECT_NO_THROW(polygons->validate());

	return polygons;
}

TEST(PolygonCollection, AddSinglePolygonFeature) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(1, 2);
	polygons.addCoordinate(2, 3);
	polygons.addCoordinate(2, 4);
	polygons.addCoordinate(1, 2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	EXPECT_EQ(1, polygons.getFeatureCount());
	EXPECT_EQ(2, polygons.start_polygon.size());
	EXPECT_EQ(2, polygons.start_ring.size());
	EXPECT_EQ(4, polygons.coordinates.size());
}

TEST(PolygonCollection, Invalid) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	EXPECT_THROW(polygons.finishRing(), FeatureException);
	EXPECT_THROW(polygons.finishPolygon(), FeatureException);
	EXPECT_THROW(polygons.finishFeature(), FeatureException);
	EXPECT_NO_THROW(polygons.validate());

	polygons.addCoordinate(1, 2);
	EXPECT_THROW(polygons.finishRing(), FeatureException);
	polygons.addCoordinate(1, 3);
	EXPECT_THROW(polygons.finishRing(), FeatureException);
	polygons.addCoordinate(2, 3);
	EXPECT_THROW(polygons.finishRing(), FeatureException);
	polygons.addCoordinate(2, 4);
	EXPECT_THROW(polygons.finishRing(), FeatureException);
	polygons.addCoordinate(1, 2);
	EXPECT_NO_THROW(polygons.finishRing());

	EXPECT_THROW(polygons.validate(), FeatureException);
	EXPECT_THROW(polygons.finishFeature(), FeatureException);

	EXPECT_NO_THROW(polygons.finishPolygon());
	EXPECT_NO_THROW(polygons.finishFeature());

	EXPECT_NO_THROW(polygons.validate());
}


TEST(PolygonCollection, Iterators) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	for (int f=0;f<10000;f++) {
		for (int p=0;p<=f%3;p++) {
			for (int r=0;r<=f%4;r++) {
				for (int c=0;c<10;c++)
					polygons.addCoordinate(f+p+r, c);
				polygons.addCoordinate(f+p+r,0);
				polygons.finishRing();
			}
			polygons.finishPolygon();
		}
		polygons.finishFeature();
	}


	double res_loop = 0;
	auto featurecount = polygons.getFeatureCount();
	for (size_t i=0;i<featurecount;i++) {
		auto startf = polygons.start_feature[i];
		auto endf = polygons.start_feature[i+1];
		for (size_t f = startf; f < endf; f++) {
			auto startp = polygons.start_polygon[f];
			auto endp = polygons.start_polygon[f+1];
			for (size_t p = startp;p < endp; p++) {
				auto startr = polygons.start_ring[p];
				auto endr = polygons.start_ring[p+1];
				for (size_t r = startr; r < endr; r++) {
					res_loop += polygons.coordinates[r].x;
				}
			}
		}
	}


	double res_iter = 0;
	for (auto feature: polygons)
		for (auto polygon : feature)
			for (auto ring : polygon)
				for (auto & c : ring)
					res_iter += c.x;

	const PolygonCollection &cpolygons = polygons;
	double res_citer = 0;
	for (auto feature: cpolygons)
		for (auto polygon : feature)
			for (auto ring : polygon)
				for (auto & c : ring)
					res_citer += c.x;

	EXPECT_EQ(res_loop, res_iter);
	EXPECT_EQ(res_loop, res_citer);
}

TEST(PolygonCollection, directReferenceAccess){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(7,7);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	EXPECT_EQ(4, polygons.getFeatureReference(0).getPolygonReference(0).getRingReference(0).size());
	EXPECT_EQ(5, polygons.getFeatureReference(1).getPolygonReference(1).getRingReference(0).size());
}


TEST(PolygonCollection, filter) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	auto &test = polygons.feature_attributes.addNumericAttribute("test", Unit::unknown());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(0, 5.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(1, 4.1);

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(2, 3.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(3, 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(polygons.filter(keep), ArgumentException);

	keep.push_back(false);
	auto polygonsFiltered = polygons.filter(keep);

	EXPECT_NO_THROW(polygonsFiltered->validate());
	EXPECT_EQ(2, polygonsFiltered->getFeatureCount());
	EXPECT_EQ(16, polygonsFiltered->coordinates.size());
	EXPECT_DOUBLE_EQ(3.1, polygonsFiltered->feature_attributes.numeric("test").get(1));
}

TEST(PolygonCollection, toWKT) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	auto &test = polygons.feature_attributes.addNumericAttribute("test", Unit::unknown());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(0, 5.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(1, 4.1);

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(2, 3.1);

	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((1 2,1 3,2 3,1 2)),MULTIPOLYGON(((1 2,1 3,2 3,1 2)),((5 8,2 3,7 6,5 8))),POLYGON((11 21,11 31,21 31,11 21),(51 81,21 31,71 61,51 81)))";
	EXPECT_EQ(wkt, polygons.toWKT());
}

TEST(PolygonCollection, toGeoJSON) {
	//TODO: test missing metadata value
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	auto &test = polygons.feature_attributes.addTextualAttribute("test", Unit::unknown());
	auto &test2 = polygons.feature_attributes.addNumericAttribute("test2", Unit::unknown());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(0, "test");
	test2.set(0, 5.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(1, "test2");
	test2.set(1, 4.1);

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(2, "test3");
	test2.set(2, 3.1);

	polygons.addDefaultTimestamps();

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[1.000000,2.000000],[1.000000,3.000000],[2.000000,3.000000],[1.000000,2.000000]]]}},{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[1.000000,2.000000],[1.000000,3.000000],[2.000000,3.000000],[1.000000,2.000000]]],[[[5.000000,8.000000],[2.000000,3.000000],[7.000000,6.000000],[5.000000,8.000000]]]]}},{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[11.000000,21.000000],[11.000000,31.000000],[21.000000,31.000000],[11.000000,21.000000]],[[51.000000,81.000000],[21.000000,31.000000],[71.000000,61.000000],[51.000000,81.000000]]]}}]})";

	EXPECT_EQ(expected, polygons.toGeoJSON(false));
}

TEST(PolygonCollection, toGeoJSONEmptyCollection) {
	//TODO: test missing metadata value
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	std::string expected = "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:1\"}},\"features\":[]}";

	EXPECT_EQ(expected, polygons.toGeoJSON(false));
}

TEST(PolygonCollection, toGeoJSONMetadata) {
	//TODO: test missing metadata value
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	auto &test = polygons.feature_attributes.addTextualAttribute("test", Unit::unknown());
	auto &test2 = polygons.feature_attributes.addNumericAttribute("test2", Unit::unknown());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(0, "test");
	test2.set(0, 5.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(1, "test2");
	test2.set(1, 4.1);

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(2, "test3");
	test2.set(2, 3.1);

	polygons.addDefaultTimestamps(0,1);

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[1.000000,2.000000],[1.000000,3.000000],[2.000000,3.000000],[1.000000,2.000000]]]},"properties":{"test":"test","test2":5.100000,"time_start":0.000000,"time_end":1.000000}},{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[1.000000,2.000000],[1.000000,3.000000],[2.000000,3.000000],[1.000000,2.000000]]],[[[5.000000,8.000000],[2.000000,3.000000],[7.000000,6.000000],[5.000000,8.000000]]]]},"properties":{"test":"test2","test2":4.100000,"time_start":0.000000,"time_end":1.000000}},{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[11.000000,21.000000],[11.000000,31.000000],[21.000000,31.000000],[11.000000,21.000000]],[[51.000000,81.000000],[21.000000,31.000000],[71.000000,61.000000],[51.000000,81.000000]]]},"properties":{"test":"test3","test2":3.100000,"time_start":0.000000,"time_end":1.000000}}]})";

	EXPECT_EQ(expected, polygons.toGeoJSON(true));
}

TEST(PolygonCollection, toARFF) {
	//TODO: test missing metadata value
	PolygonCollection polygons(SpatioTemporalReference(
		SpatialReference::unreferenced(),
		TemporalReference(TIMETYPE_UNIX)
	));

	auto &test = polygons.feature_attributes.addTextualAttribute("test", Unit::unknown());
	auto &test2 = polygons.feature_attributes.addNumericAttribute("test2", Unit::unknown());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(0, "test");
	test2.set(0, 5.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(1, "test2");
	test2.set(1, 4.1);

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	test.set(2, "test3");
	test2.set(2, 3.1);


	polygons.addDefaultTimestamps();

	std::string expected = "@RELATION export\n"
			"\n"
			"@ATTRIBUTE wkt STRING\n"
			"@ATTRIBUTE time_start DATE\n"
			"@ATTRIBUTE time_end DATE\n"
			"@ATTRIBUTE test STRING\n"
			"@ATTRIBUTE test2 NUMERIC\n"
			"\n"
			"@DATA\n"
			"\"POLYGON((1 2,1 3,2 3,1 2))\",\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"test\",5.1\n"
			"\"MULTIPOLYGON(((1 2,1 3,2 3,1 2)),((5 8,2 3,7 6,5 8)))\",\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"test2\",4.1\n"
			"\"POLYGON((11 21,11 31,21 31,11 21),(51 81,21 31,71 61,51 81))\",\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"test3\",3.1\n";

	EXPECT_EQ(expected, polygons.toARFF());
}

TEST(PolygonCollection, calculateMBR) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(35,10);
	polygons.addCoordinate(45,45);
	polygons.addCoordinate(15,40);
	polygons.addCoordinate(10,20);
	polygons.addCoordinate(35,10);
	polygons.finishRing();
	polygons.addCoordinate(20,30);
	polygons.addCoordinate(35,35);
	polygons.addCoordinate(30,20);
	polygons.addCoordinate(20,30);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	auto mbr = polygons.getCollectionMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(45, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(45, mbr.y2);

	mbr = polygons.getFeatureReference(0).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(2, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(3, mbr.y2);

	mbr = polygons.getFeatureReference(1).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(7, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(8, mbr.y2);

	mbr = polygons.getFeatureReference(2).getMBR();
	EXPECT_DOUBLE_EQ(10, mbr.x1);
	EXPECT_DOUBLE_EQ(45, mbr.x2);
	EXPECT_DOUBLE_EQ(10, mbr.y1);
	EXPECT_DOUBLE_EQ(45, mbr.y2);


	mbr = polygons.getFeatureReference(1).getPolygonReference(0).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(2, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(3, mbr.y2);

	mbr = polygons.getFeatureReference(1).getPolygonReference(1).getMBR();
	EXPECT_DOUBLE_EQ(2, mbr.x1);
	EXPECT_DOUBLE_EQ(7, mbr.x2);
	EXPECT_DOUBLE_EQ(3, mbr.y1);
	EXPECT_DOUBLE_EQ(8, mbr.y2);
}

TEST(PolygonCollection, pointInPolygon){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(1,5);
	polygons.addCoordinate(3,3);
	polygons.addCoordinate(5,3);
	polygons.addCoordinate(6,5);
	polygons.addCoordinate(7,1.5);
	polygons.addCoordinate(4,0);
	polygons.addCoordinate(2,1);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(1,5);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	Coordinate a(4, 2); //inside
	Coordinate b(2, 3); //inside, collinear to edge
	Coordinate c(4, 5); //outside, in line of two vertices
	Coordinate d(2, 0); //outside
	Coordinate e(2, 4); //on edge
	Coordinate f(2.05, 4); //next to edge (out)
	Coordinate g(1.95, 4); //next to edge (in)


	EXPECT_EQ(true, polygons.pointInRing(a, 0, 9));
	EXPECT_EQ(true, polygons.pointInRing(b, 0, 9));
	EXPECT_EQ(false, polygons.pointInRing(c, 0, 9));
	EXPECT_EQ(false, polygons.pointInRing(d, 0, 9));
	EXPECT_EQ(true, polygons.pointInRing(e, 0, 9));
	EXPECT_EQ(false, polygons.pointInRing(f, 0, 9));
	EXPECT_EQ(true, polygons.pointInRing(g, 0, 9));
}

TEST(PolygonCollection, pointInPolygonWithHole){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(20,20);
	polygons.addCoordinate(20,30);
	polygons.addCoordinate(30,30);
	polygons.addCoordinate(30,20);
	polygons.addCoordinate(20,20);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(0,0);
	polygons.addCoordinate(10,0);
	polygons.addCoordinate(10,10);
	polygons.addCoordinate(0,10);
	polygons.addCoordinate(0,0);
	polygons.finishRing();
	polygons.addCoordinate(1,5);
	polygons.addCoordinate(3,3);
	polygons.addCoordinate(5,3);
	polygons.addCoordinate(6,5);
	polygons.addCoordinate(7,1.5);
	polygons.addCoordinate(4,0);
	polygons.addCoordinate(2,1);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(1,5);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	//following points with respect to hole
	Coordinate a(4, 2); //inside
	Coordinate b(2, 3); //inside, collinear to edge
	Coordinate c(4, 5); //outside, in line of two vertices
	Coordinate d(2, 0); //outside
	Coordinate e(2, 4); //on edge
	Coordinate f(2.05, 4); //next to edge (out)
	Coordinate g(1.95, 4); //next to edge (in)


	EXPECT_EQ(false, polygons.pointInCollection(a));
	EXPECT_EQ(false, polygons.pointInCollection(b));
	EXPECT_EQ(true, polygons.pointInCollection(c));
	//EXPECT_EQ(true, polygons.pointInCollection(d)); //algo can't handle this case
	EXPECT_EQ(false, polygons.pointInCollection(e));
	EXPECT_EQ(true, polygons.pointInCollection(f));
	EXPECT_EQ(false, polygons.pointInCollection(g));
}

TEST(PolygonCollection, bulkPointInPolygon){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.addCoordinate(20,20);
	polygons.addCoordinate(20,30);
	polygons.addCoordinate(30,30);
	polygons.addCoordinate(30,20);
	polygons.addCoordinate(20,20);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(0,0);
	polygons.addCoordinate(10,0);
	polygons.addCoordinate(10,10);
	polygons.addCoordinate(0,10);
	polygons.addCoordinate(0,0);
	polygons.finishRing();
	polygons.addCoordinate(1,5);
	polygons.addCoordinate(3,3);
	polygons.addCoordinate(5,3);
	polygons.addCoordinate(6,5);
	polygons.addCoordinate(7,1.5);
	polygons.addCoordinate(4,0);
	polygons.addCoordinate(2,1);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(1,5);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	auto tester = polygons.getPointInCollectionBulkTester();

	Coordinate a(4, 5);
	Coordinate b(4, 2);

	EXPECT_EQ(true, tester.pointInCollection(a));
	EXPECT_EQ(false, tester.pointInCollection(b));
}

TEST(PolygonCollection, WKTImport){
	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((10 20, 30 30, 0 30, 10 20), (2 2, 5 2, 1 1, 2 2)))";
	auto polygons = WKBUtil::readPolygonCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, polygons->getFeatureCount());
	EXPECT_EQ(1, polygons->getFeatureReference(0).size());
	EXPECT_EQ(2, polygons->getFeatureReference(0).getPolygonReference(0).size());
}

TEST(PolygonCollection, WKTImportMultiPolygon){
	std::string wkt = "GEOMETRYCOLLECTION(MULTIPOLYGON(((1 2, 3 3, 0 3, 1 2)), ((7 8, 9 10, 11 12, 13 14, 7 8))))";
	auto polygons = WKBUtil::readPolygonCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, polygons->getFeatureCount());
	EXPECT_EQ(2, polygons->getFeatureReference(0).size());
}

TEST(PolygonCollection, WKTImportMixed){
	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((10 20, 30 30, 0 30, 10 20), (2 2, 5 2, 1 1, 2 2)), MULTIPOLYGON(((1 2, 3 3, 0 3, 1 2)), ((7 8, 9 10, 11 12, 13 14, 7 8))))";
	auto polygons = WKBUtil::readPolygonCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(2, polygons->getFeatureCount());
	EXPECT_EQ(2, polygons->getFeatureReference(0).getPolygonReference(0).size());
	EXPECT_EQ(2, polygons->getFeatureReference(1).size());
}

TEST(PolygonCollection, WKTAddSingleFeature){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	polygons.addCoordinate(20,20);
	polygons.addCoordinate(20,30);
	polygons.addCoordinate(30,30);
	polygons.addCoordinate(30,20);
	polygons.addCoordinate(20,20);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	std::string wkt = "POLYGON((10 20, 30 30, 0 30, 10 20), (2 2, 5 2, 1 1, 2 2))";
	WKBUtil::addFeatureToCollection(polygons, wkt);

	EXPECT_EQ(2, polygons.getFeatureCount());
	EXPECT_EQ(2, polygons.getFeatureReference(1).getPolygonReference(0).size());
}

TEST(PolygonCollection, WKTAddMultiFeature){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	polygons.addCoordinate(20,20);
	polygons.addCoordinate(20,30);
	polygons.addCoordinate(30,30);
	polygons.addCoordinate(30,20);
	polygons.addCoordinate(20,20);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	std::string wkt = "MULTIPOLYGON(((1 2, 3 3, 0 3, 1 2)), ((7 8, 9 10, 11 12, 13 14, 7 8)))";
	WKBUtil::addFeatureToCollection(polygons, wkt);

	EXPECT_EQ(2, polygons.getFeatureCount());
	EXPECT_EQ(2, polygons.getFeatureReference(1).size());
}

TEST(PolygonCollection, WKTAddFeatureFail){
	auto polygons = createPolygonsWithAttributesAndTime();
	std::string wkt = "POINT(3 foo)";
	EXPECT_ANY_THROW(WKBUtil::addFeatureToCollection(*polygons, wkt));

	auto result = createPolygonsWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *polygons);
}

std::unique_ptr<PolygonCollection> createPolygonsForSTRefFilter(){
	auto stref = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 100, 100),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 100));

	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((1 2, 5 5, 8 9, 1 2)), POLYGON((5 5, 10 12, 13 4, 5 5)), POLYGON((10 0, 10 10, 12 14, 10 0)), POLYGON((10 0, 10 10, 12 14, 10 0)), POLYGON((30 30, 33 12, 44 18, 30 30)), POLYGON((-5 -5, 15 -5, 15 15, -5 15, -5 -5), (-1 -1, 11 -1, 11 11, -1 11, -1 -1)), MULTIPOLYGON(((1 1, 1 9, 9 9, 9 9, 1 1)), ((11 11, 11 99, 99 99, 99 11, 11 11))))";
	auto lines = WKBUtil::readPolygonCollection(wkt, stref);

	EXPECT_NO_THROW(lines->validate());

	return lines;
}

TEST(PolygonCollection, filterBySTRefIntersection){
	const auto polygons = createPolygonsForSTRefFilter();

	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));

	auto filtered = polygons->filterBySpatioTemporalReferenceIntersection(filter);

	std::vector<bool> keep = {true, true, true, true, false, false, true};
	auto expected = polygons->filter(keep);
	expected->replaceSTRef(filter);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(PolygonCollection, filterBySTRefIntersectionWithTime){
	const auto polygons = createPolygonsForSTRefFilter();
	polygons->time_start = {1,  5,  9, 15, 30, 1,  1};
	polygons->time_end =   {9, 12, 11, 80, 44, 6, 99};

	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));

	auto filtered = polygons->filterBySpatioTemporalReferenceIntersection(filter);

	std::vector<bool> keep = {true, true, true, false, false, false, true};
	auto expected = polygons->filter(keep);
	expected->replaceSTRef(filter);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(PolygonCollection, filterBySTRefIntersectionInPlace){
	const auto polygons = createPolygonsForSTRefFilter();


	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));
	std::vector<bool> keep = {true, true, true, true, false, false, true};
	auto expected = polygons->filter(keep);
	expected->replaceSTRef(filter);

	polygons->filterBySpatioTemporalReferenceIntersectionInPlace(filter);



	CollectionTestUtil::checkEquality(*expected, *polygons);
}

TEST(PolygonCollection, filterInPlace){
	auto polygons = createPolygonsWithAttributesAndTime();

	std::vector<bool> keep = {true, false, true, false};
	auto expected = polygons->filter(keep);

	polygons->filterInPlace(keep);

	CollectionTestUtil::checkEquality(*expected, *polygons);
}

TEST(PolygonCollection, filterByPredicate){
	auto polygons = createPolygonsWithAttributesAndTime();

	auto filtered = polygons->filter([](const PolygonCollection &c, size_t feature) {
		return c.time_start[feature] >= 8;
	});

	std::vector<bool> keep({false, false, true, true});
	auto expected = polygons->filter(keep);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(PolygonCollection, filterByPredicateInPlace){
	auto polygons = createPolygonsWithAttributesAndTime();

	std::vector<bool> keep({false, false, true, true});
	auto expected = polygons->filter(keep);

	polygons->filterInPlace([](PolygonCollection &c, size_t feature) {
		return c.time_start[feature] >= 8;
	});

	CollectionTestUtil::checkEquality(*expected, *polygons);
}

TEST(PolygonCollection, StreamSerialization){
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	//TODO: attributes, time array

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.addCoordinate(5,8);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(7,6);
	polygons.addCoordinate(5,8);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	polygons.addCoordinate(11,21);
	polygons.addCoordinate(11,31);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(11,21);
	polygons.finishRing();
	polygons.addCoordinate(51,81);
	polygons.addCoordinate(21,31);
	polygons.addCoordinate(71,61);
	polygons.addCoordinate(51,81);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	//create binarystream using pipe
	int fds[2];
	int status = pipe2(fds, O_NONBLOCK | O_CLOEXEC);
	EXPECT_EQ(0, status);

	BinaryFDStream stream(fds[0], fds[1]);

	polygons.toStream(stream);

	PolygonCollection polygons2(stream);

	CollectionTestUtil::checkEquality(polygons, polygons2);
}

TEST(PolygonCollection, removeLastFeature){
	auto polygons = createPolygonsWithAttributesAndTime();

	polygons->removeLastFeature();
	polygons->validate();

	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((10 10, 10 30, 25 20, 10 10)), POLYGON((15 70, 25 90, 45 90, 40 80, 50 70, 15 70), (30 75, 25 80, 30 85, 35 80, 30 75)), POLYGON((50 30, 65 60, 100 25, 50 30), (55 35, 65 45, 65 35, 55 35), (75 30, 75 35, 85 35, 85 30, 75 30)))";
	auto result = WKBUtil::readPolygonCollection(wkt, SpatioTemporalReference::unreferenced());
	result->time_start = {2, 4,  8};
	result->time_end =   {4, 8, 16};

	result->global_attributes.setTextual("info", "1234");
	result->global_attributes.setNumeric("index", 42);

	result->feature_attributes.addNumericAttribute("value", Unit::unknown(), {0.0, 1.1, 2.2});
	result->feature_attributes.addTextualAttribute("label", Unit::unknown(), {"l0", "l1", "l2"});


	result->validate();

	CollectionTestUtil::checkEquality(*result, *polygons);
}

TEST(PolygonCollection, removeLastFeatureUnfinishedRing){
	auto polygons = createPolygonsWithAttributesAndTime();

	polygons->addCoordinate(2,3);
	polygons->addCoordinate(3,3);
	polygons->addCoordinate(5,3);
	polygons->addCoordinate(2,3);
	polygons->feature_attributes.textual("label").set(polygons->getFeatureCount(), "fail");

	polygons->removeLastFeature();
	polygons->validate();

	auto result = createPolygonsWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *polygons);
}

TEST(PolygonCollection, removeLastFeatureUnfinishedPolygon){
	auto polygons = createPolygonsWithAttributesAndTime();

	polygons->addCoordinate(2,3);
	polygons->addCoordinate(3,3);
	polygons->addCoordinate(5,3);
	polygons->addCoordinate(2,3);
	polygons->finishRing();
	polygons->feature_attributes.textual("label").set(polygons->getFeatureCount(), "fail");

	polygons->removeLastFeature();
	polygons->validate();

	auto result = createPolygonsWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *polygons);
}

TEST(PolygonCollection, removeLastFeatureUnfinishedFeature){
	auto polygons = createPolygonsWithAttributesAndTime();

	polygons->addCoordinate(2,3);
	polygons->addCoordinate(3,3);
	polygons->addCoordinate(5,3);
	polygons->addCoordinate(2,3);
	polygons->finishRing();
	polygons->finishPolygon();
	polygons->feature_attributes.textual("label").set(polygons->getFeatureCount(), "fail");

	polygons->removeLastFeature();
	polygons->validate();

	auto result = createPolygonsWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *polygons);
}
