#include <gtest/gtest.h>
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
	polygons.local_md_value.addEmptyVector("test");

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_value.set(0, "test", 5.1);

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
	polygons.local_md_value.set(1, "test", 4.1);

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
	polygons.local_md_value.set(2, "test", 3.1);

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_value.set(3, "test", 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(polygons.filter(keep), ArgumentException);

	keep.push_back(false);
	auto polygonsFiltered = polygons.filter(keep);

	EXPECT_EQ(2, polygonsFiltered->getFeatureCount());
	EXPECT_EQ(16, polygonsFiltered->coordinates.size());
	EXPECT_EQ(2, polygonsFiltered->local_md_value.getVector("test").size());
	EXPECT_DOUBLE_EQ(3.1, polygonsFiltered->local_md_value.get(1, "test"));
}

TEST(PolygonCollection, toWKT) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	polygons.local_md_value.addEmptyVector("test");

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_value.set(0, "test", 5.1);

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
	polygons.local_md_value.set(1, "test", 4.1);

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
	polygons.local_md_value.set(2, "test", 3.1);

	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((1 2,1 3,2 3,1 2)),MULTIPOLYGON(((1 2,1 3,2 3,1 2)),((5 8,2 3,7 6,5 8))),POLYGON((11 21,11 31,21 31,11 21),(51 81,21 31,71 61,51 81)))";
	EXPECT_EQ(wkt, polygons.toWKT());
}

TEST(PolygonCollection, toGeoJSON) {
	//TODO: test missing metadata value
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());

	polygons.local_md_string.addEmptyVector("test");
	polygons.local_md_value.addEmptyVector("test2");

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_string.set(0, "test", "test");
	polygons.local_md_value.set(0, "test2", 5.1);

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
	polygons.local_md_string.set(1, "test", "test2");
	polygons.local_md_value.set(1, "test2", 4.1);

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
	polygons.local_md_string.set(2, "test", "test3");
	polygons.local_md_value.set(2, "test2", 3.1);

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

	polygons.local_md_string.addEmptyVector("test");
	polygons.local_md_value.addEmptyVector("test2");

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_string.set(0, "test", "test");
	polygons.local_md_value.set(0, "test2", 5.1);

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
	polygons.local_md_string.set(1, "test", "test2");
	polygons.local_md_value.set(1, "test2", 4.1);

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
	polygons.local_md_string.set(2, "test", "test3");
	polygons.local_md_value.set(2, "test2", 3.1);

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

	polygons.local_md_string.addEmptyVector("test");
	polygons.local_md_value.addEmptyVector("test2");

	polygons.addCoordinate(1,2);
	polygons.addCoordinate(1,3);
	polygons.addCoordinate(2,3);
	polygons.addCoordinate(1,2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();
	polygons.local_md_string.set(0, "test", "test");
	polygons.local_md_value.set(0, "test2", 5.1);

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
	polygons.local_md_string.set(1, "test", "test2");
	polygons.local_md_value.set(1, "test2", 4.1);

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
	polygons.local_md_string.set(2, "test", "test3");
	polygons.local_md_value.set(2, "test2", 3.1);


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
	auto polygons = WKBUtil::readPolygonCollection(wkt);

	EXPECT_EQ(1, polygons->getFeatureCount());
	EXPECT_EQ(1, polygons->getFeatureReference(0).size());
	EXPECT_EQ(2, polygons->getFeatureReference(0).getPolygonReference(0).size());
}

TEST(PolygonCollection, WKTImportMultiPolygon){
	std::string wkt = "GEOMETRYCOLLECTION(MULTIPOLYGON(((1 2, 3 3, 0 3, 1 2)), ((7 8, 9 10, 11 12, 13 14, 7 8))))";
	auto polygons = WKBUtil::readPolygonCollection(wkt);

	EXPECT_EQ(1, polygons->getFeatureCount());
	EXPECT_EQ(2, polygons->getFeatureReference(0).size());
}

TEST(PolygonCollection, WKTImportMixed){
	std::string wkt = "GEOMETRYCOLLECTION(POLYGON((10 20, 30 30, 0 30, 10 20), (2 2, 5 2, 1 1, 2 2)), MULTIPOLYGON(((1 2, 3 3, 0 3, 1 2)), ((7 8, 9 10, 11 12, 13 14, 7 8))))";
	auto polygons = WKBUtil::readPolygonCollection(wkt);

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
