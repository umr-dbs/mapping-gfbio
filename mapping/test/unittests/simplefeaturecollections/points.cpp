#include <gtest/gtest.h>
#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <vector>
#include <json/json.h>
#include <unistd.h>
#include <fcntl.h>
#include "util/binarystream.h"

#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"
#include "raster/opencl.h"

void checkEquality(const PointCollection& a, const PointCollection& b){
	//TODO: check global attributes equality

	EXPECT_EQ(a.stref.epsg, b.stref.epsg);
	EXPECT_EQ(a.stref.timetype, b.stref.timetype);
	EXPECT_EQ(a.stref.t1, b.stref.t1);
	EXPECT_EQ(a.stref.t2, b.stref.t2);
	EXPECT_EQ(a.stref.epsg, b.stref.epsg);
	EXPECT_EQ(a.stref.x1, b.stref.x1);
	EXPECT_EQ(a.stref.y1, b.stref.y1);
	EXPECT_EQ(a.stref.x2, b.stref.x2);
	EXPECT_EQ(a.stref.y2, b.stref.y2);

	EXPECT_EQ(a.getFeatureCount(), b.getFeatureCount());
	EXPECT_EQ(a.hasTime(), b.hasTime());

	for(size_t feature = 0; feature < a.getFeatureCount(); ++feature){
		EXPECT_EQ(a.getFeatureReference(feature).size(), b.getFeatureReference(feature).size());
		if(a.hasTime()){
			EXPECT_EQ(a.time_start[feature], b.time_start[feature]);
			EXPECT_EQ(a.time_end[feature], b.time_end[feature]);
		}

		//TODO: check feature attributes equality

		for(size_t point = a.start_feature[feature]; point < a.start_feature[feature + 1]; ++point){
			EXPECT_EQ(a.coordinates[point].x, b.coordinates[point].x);
			EXPECT_EQ(a.coordinates[point].y, b.coordinates[point].y);
		}
	}
}

TEST(PointCollection, AddSinglePointFeature) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addSinglePointFeature(Coordinate(1,2));

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates.size());
	EXPECT_EQ(1, points.coordinates[0].x);
	EXPECT_EQ(2, points.coordinates[0].y);
}

TEST(PointCollection, AddSinglePointFeature2) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.finishFeature();

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates.size());
	EXPECT_DOUBLE_EQ(1, points.coordinates[0].x);
	EXPECT_DOUBLE_EQ(2, points.coordinates[0].y);
}

TEST(PointCollection, AddMultiPointFeature) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(2, points.coordinates.size());
}

TEST(PointCollection, MixedFeatures) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	points.addCoordinate(3,4);
	points.finishFeature();

	EXPECT_EQ(2, points.getFeatureCount());
	EXPECT_EQ(3, points.coordinates.size());
}

TEST(PointCollection, EmptyFeature) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	EXPECT_THROW(points.finishFeature(), FeatureException);
}

TEST(PointCollection, UnfinishedFeature){
	PointCollection points(SpatioTemporalReference::unreferenced());

	points.addCoordinate(1, 2);

	EXPECT_THROW(points.validate(), FeatureException);

	points.finishFeature();
	EXPECT_NO_THROW(points.validate());
}

//if this test fails, it could just mean the JSON format changed, not that it is invalid/wrong
TEST(PointCollection, toGeoJSON) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.finishFeature();
	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1.000000,2.000000]}},{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[2.000000,3.000000],[3.000000,4.000000]]}}]})";
	EXPECT_EQ(expected, points.toGeoJSON(false));
}

//if this test fails, it could just mean the JSON format changed, not that it is invalid/wrong
TEST(PointCollection, toGeoJSONEmptyCollection) {
	PointCollection points(SpatioTemporalReference::unreferenced());

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[]})";
	EXPECT_EQ(expected, points.toGeoJSON(false));
}

//if this test fails, it could just mean the JSON format changed, not that it is invalid/wrong
TEST(PointCollection, toGeoJSONWithMetadata) {
	PointCollection points(SpatioTemporalReference::unreferenced());

	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);

	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();
	test.set(1, 2.1);

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1.000000,2.000000]},"properties":{"test":5.100000}},{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[2.000000,3.000000],[3.000000,4.000000]]},"properties":{"test":2.100000}}]})";
	EXPECT_EQ(expected, points.toGeoJSON(true));
}


//representative for all featurecollections, as serialization of metadata is done identically
TEST(PointCollection, toGeoJSONStringEscaping) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addTextualAttribute("test", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, "Simple \\nTest \" of ,.;:--//\t\0 symbols");

	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;

	std::istringstream iss(points.toGeoJSON(true));
	bool success = reader.parse(iss, root);

	EXPECT_EQ(true, success);
}


TEST(PointCollection, toCSV) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 2.1);

	std::string expected = "feature,lon,lat,\"test\"\n0,1.000000,2.000000,5.100000\n1,1.000000,2.000000,2.100000\n1,2.000000,3.000000,2.100000\n";
	EXPECT_EQ(expected, points.toCSV());
}

TEST(PointCollection, toWKT) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 2.1);

	std::string expected = "GEOMETRYCOLLECTION(POINT(1 2),MULTIPOINT((1 2),(2 3)))";
	EXPECT_EQ(expected, points.toWKT());
}

TEST(PointCollection, SimpletoARFF) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());
	auto &test2 = points.feature_attributes.addTextualAttribute("test2", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);
	test2.set(0, "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 2.1);
	test2.set(1, "TEST1234");

	std::string expected = "@RELATION export\n"
			"\n"
			"@ATTRIBUTE longitude NUMERIC\n"
			"@ATTRIBUTE latitude NUMERIC\n"
			"@ATTRIBUTE test2 STRING\n"
			"@ATTRIBUTE test NUMERIC\n"
			"\n"
			"@DATA\n"
			"1,2,\"TEST123\",5.1\n"
			"2,3,\"TEST1234\",2.1\n";
	EXPECT_EQ(expected, points.toARFF());
}

TEST(PointCollection, SimpletoARFFWithTime) {
	//TODO: test missing metadata value
	TemporalReference tref(TIMETYPE_UNIX);
	SpatioTemporalReference stref(SpatialReference::unreferenced(), tref);
	PointCollection points(stref);//is there a better way to initialize?

	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());
	auto &test2 = points.feature_attributes.addTextualAttribute("test2", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);
	test2.set(0, "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 2.1);
	test2.set(1, "TEST1234");

	points.addDefaultTimestamps();

	std::string expected = "@RELATION export\n"
			"\n"
			"@ATTRIBUTE longitude NUMERIC\n"
			"@ATTRIBUTE latitude NUMERIC\n"
			"@ATTRIBUTE time_start DATE\n"
			"@ATTRIBUTE time_end DATE\n"
			"@ATTRIBUTE test2 STRING\n"
			"@ATTRIBUTE test NUMERIC\n"
			"\n"
			"@DATA\n"
			"1,2,\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"TEST123\",5.1\n"
			"2,3,\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"TEST1234\",2.1\n";
	EXPECT_EQ(expected, points.toARFF());
}

TEST(PointCollection, NonSimpletoARFF) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());
	auto &test2 = points.feature_attributes.addTextualAttribute("test2", Unit::unknown());

	points.addCoordinate(1,2);
	points.addCoordinate(2,2);
	points.finishFeature();
	test.set(0, 5.1);
	test2.set(0, "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 2.1);
	test2.set(1, "TEST1234");

	std::string expected = "@RELATION export\n"
			"\n"
			"@ATTRIBUTE feature NUMERIC\n"
			"@ATTRIBUTE longitude NUMERIC\n"
			"@ATTRIBUTE latitude NUMERIC\n"
			"@ATTRIBUTE test2 STRING\n"
			"@ATTRIBUTE test NUMERIC\n"
			"\n"
			"@DATA\n"
			"0,1,2,\"TEST123\",5.1\n"
			"0,2,2,\"TEST123\",5.1\n"
			"1,2,3,\"TEST1234\",2.1\n";
	EXPECT_EQ(expected, points.toARFF());
}

TEST(PointCollection, filter) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	auto &test = points.feature_attributes.addNumericAttribute("test", Unit::unknown());

	points.addCoordinate(1,2);
	points.finishFeature();
	test.set(0, 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	test.set(1, 4.1);

	points.addCoordinate(3,4);
	points.finishFeature();
	test.set(2, 3.1);

	points.addCoordinate(5,6);
	points.addCoordinate(6,7);
	points.finishFeature();
	test.set(3, 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(points.filter(keep), ArgumentException);

	keep.push_back(false);
	auto pointsFiltered = points.filter(keep);

	EXPECT_NO_THROW(pointsFiltered->validate());
	EXPECT_EQ(2, pointsFiltered->getFeatureCount());
	EXPECT_EQ(3, pointsFiltered->coordinates.size());
	EXPECT_DOUBLE_EQ(3.1, pointsFiltered->feature_attributes.numeric("test").get(1));
}

TEST(PointCollection, Iterators) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	for (size_t i=0;i<100000;i++) {
		points.addCoordinate(i,i+1);
		if (i % 3 == 0)
			points.addCoordinate(i,i+2);
		points.finishFeature();
	}

	double res_loop = 0;
	auto featurecount = points.getFeatureCount();
	for (size_t i=0;i<featurecount;i++) {
		auto start = points.start_feature[i];
		auto end = points.start_feature[i+1];
		for (size_t j = start; j < end; j++) {
			res_loop += points.coordinates[j].x;
		}
	}

	double res_iter = 0;
	for (auto feature : points) {
		for (auto & c : feature) {
			res_iter += c.x;
		}
	}

	double res_citer = 0;
	const PointCollection &cpoints = points;
	for (auto feature : cpoints) {
		for (auto & c : feature) {
			res_citer += c.x;
		}
	}

	EXPECT_EQ(res_loop, res_iter);
	EXPECT_EQ(res_loop, res_citer);
}

TEST(PointCollection, directReferenceAccess){
	PointCollection points(SpatioTemporalReference::unreferenced());

	points.addCoordinate(1,2);
	points.finishFeature();

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();

	EXPECT_EQ(1, points.getFeatureReference(0).size());
	EXPECT_EQ(2, points.getFeatureReference(1).size());
}

TEST(PointCollection, calulcateMBR){
	PointCollection points(SpatioTemporalReference::unreferenced());

	points.addCoordinate(0,0);
	points.finishFeature();

	points.addCoordinate(1,1);
	points.addCoordinate(1,2);
	points.finishFeature();

	points.addCoordinate(1,1);
	points.addCoordinate(1,2);
	points.addCoordinate(0,2);
	points.finishFeature();

	auto mbr = points.getCollectionMBR();
	EXPECT_DOUBLE_EQ(0, mbr.x1);
	EXPECT_DOUBLE_EQ(1, mbr.x2);
	EXPECT_DOUBLE_EQ(0, mbr.y1);
	EXPECT_DOUBLE_EQ(2, mbr.y2);

	mbr = points.getFeatureReference(0).getMBR();
	EXPECT_DOUBLE_EQ(0, mbr.x1);
	EXPECT_DOUBLE_EQ(0, mbr.x2);
	EXPECT_DOUBLE_EQ(0, mbr.y1);
	EXPECT_DOUBLE_EQ(0, mbr.y2);

	mbr = points.getFeatureReference(1).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(1, mbr.x2);
	EXPECT_DOUBLE_EQ(1, mbr.y1);
	EXPECT_DOUBLE_EQ(2, mbr.y2);

	mbr = points.getFeatureReference(2).getMBR();
	EXPECT_DOUBLE_EQ(0, mbr.x1);
	EXPECT_DOUBLE_EQ(1, mbr.x2);
	EXPECT_DOUBLE_EQ(1, mbr.y1);
	EXPECT_DOUBLE_EQ(2, mbr.y2);
}

TEST(PointCollection, WKTImport){
	std::string wkt = "GEOMETRYCOLLECTION(POINT(1 2))";
	auto points = WKBUtil::readPointCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, points->getFeatureCount());
	EXPECT_EQ(1, points->coordinates[0].x);
	EXPECT_EQ(2, points->coordinates[0].y);
}

TEST(PointCollection, WKTImportMultiPoint){
	std::string wkt = "GEOMETRYCOLLECTION(MULTIPOINT(1 2, 3 4))";
	auto points = WKBUtil::readPointCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, points->getFeatureCount());
	EXPECT_EQ(1, points->coordinates[0].x);
	EXPECT_EQ(2, points->coordinates[0].y);
	EXPECT_EQ(3, points->coordinates[1].x);
	EXPECT_EQ(4, points->coordinates[1].y);
}

TEST(PointCollection, WKTImportMixed){
	std::string wkt = "GEOMETRYCOLLECTION(POINT(1 2), MULTIPOINT(1 2, 3 4))";
	auto points = WKBUtil::readPointCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(2, points->getFeatureCount());
	EXPECT_EQ(1, points->coordinates[0].x);
	EXPECT_EQ(2, points->coordinates[0].y);
	EXPECT_EQ(1, points->coordinates[1].x);
	EXPECT_EQ(2, points->coordinates[1].y);
	EXPECT_EQ(3, points->coordinates[2].x);
	EXPECT_EQ(4, points->coordinates[2].y);
	EXPECT_EQ(3, points->coordinates[2].x);
	EXPECT_EQ(1, points->start_feature[1]);
}

TEST(PointCollection, WKTAddSingleFeature){
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addSinglePointFeature(Coordinate(1, 2));
	std::string wkt = "POINT(3 4)";
	WKBUtil::addFeatureToCollection(points, wkt);

	EXPECT_EQ(2, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates[0].x);
	EXPECT_EQ(2, points.coordinates[0].y);
	EXPECT_EQ(3, points.coordinates[1].x);
	EXPECT_EQ(4, points.coordinates[1].y);
}

TEST(PointCollection, WKTAddMultiFeature){
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addSinglePointFeature(Coordinate(1, 2));
	std::string wkt = "MULTIPOINT(3 4, 5 6)";
	WKBUtil::addFeatureToCollection(points, wkt);

	EXPECT_EQ(2, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates[0].x);
	EXPECT_EQ(2, points.coordinates[0].y);
	EXPECT_EQ(3, points.coordinates[1].x);
	EXPECT_EQ(4, points.coordinates[1].y);
	EXPECT_EQ(5, points.coordinates[2].x);
	EXPECT_EQ(6, points.coordinates[2].y);
}

TEST(PointCollection, FilterByRectangleIntersection){
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.addSinglePointFeature(Coordinate(1,1));
	points.addCoordinate(11, 11);
	points.addCoordinate(12, 11);
	points.finishFeature();

	points.addCoordinate(9, 9);
	points.addCoordinate(15, 14);
	points.finishFeature();

	points.addSinglePointFeature(Coordinate(2,3));

	points.addSinglePointFeature(Coordinate(20,20));

	auto filteredPoints = points.filterByRectangleIntersection(0, 0, 10, 10);

	EXPECT_EQ(3, filteredPoints->getFeatureCount());
	EXPECT_EQ(1, filteredPoints->getFeatureReference(0).size());
	EXPECT_EQ(1, filteredPoints->coordinates[0].x);
	EXPECT_EQ(1, filteredPoints->coordinates[0].y);
	EXPECT_EQ(2, filteredPoints->getFeatureReference(1).size());
	EXPECT_EQ(9, filteredPoints->coordinates[1].x);
	EXPECT_EQ(9, filteredPoints->coordinates[1].y);
	EXPECT_EQ(15, filteredPoints->coordinates[2].x);
	EXPECT_EQ(14, filteredPoints->coordinates[2].y);
	EXPECT_EQ(1, filteredPoints->getFeatureReference(2).size());
	EXPECT_EQ(2, filteredPoints->coordinates[3].x);
	EXPECT_EQ(3, filteredPoints->coordinates[3].y);
}

TEST(PointCollection, StreamSerialization){
	PointCollection points(SpatioTemporalReference::unreferenced());

	//TODO attributes and time

	points.addSinglePointFeature(Coordinate(1,1));
	points.addCoordinate(11, 11);
	points.addCoordinate(12, 11);
	points.finishFeature();

	points.addCoordinate(9, 9);
	points.addCoordinate(15, 14);
	points.finishFeature();

	points.addSinglePointFeature(Coordinate(2,3));

	points.addSinglePointFeature(Coordinate(20,20));

	//create binarystream using pipe
	int fds[2];
	int status = pipe2(fds, O_NONBLOCK | O_CLOEXEC);
	EXPECT_EQ(0, status);

	UnixSocket stream(fds[0], fds[1]);

	points.toStream(stream);

	PointCollection points2(stream);

	checkEquality(points, points2);
}
