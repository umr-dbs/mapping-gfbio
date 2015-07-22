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

TEST(PointCollection, AddSinglePointFeature) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	points.addSinglePointFeature(Coordinate(1,2));

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates.size());
	EXPECT_EQ(1, points.coordinates[0].x);
	EXPECT_EQ(2, points.coordinates[0].y);
}

TEST(PointCollection, AddSinglePointFeature2) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.finishFeature();

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(1, points.coordinates.size());
	EXPECT_DOUBLE_EQ(1, points.coordinates[0].x);
	EXPECT_DOUBLE_EQ(2, points.coordinates[0].y);
}

TEST(PointCollection, AddMultiPointFeature) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();

	EXPECT_EQ(1, points.getFeatureCount());
	EXPECT_EQ(2, points.coordinates.size());
}

TEST(PointCollection, MixedFeatures) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	points.addCoordinate(3,4);
	points.finishFeature();

	EXPECT_EQ(2, points.getFeatureCount());
	EXPECT_EQ(3, points.coordinates.size());
}

TEST(PointCollection, EmptyFeature) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	EXPECT_THROW(points.finishFeature(), FeatureException);
}

//if this test fails, it could just mean the JSON format changed, not that it is invalid/wrong
TEST(PointCollection, toGeoJSON) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());
	points.addCoordinate(1,2);
	points.finishFeature();
	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1.000000,2.000000]]}},{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[2.000000,3.000000],[3.000000,4.000000]]}}]})";
	EXPECT_EQ(expected, points.toGeoJSON(false));
}

//if this test fails, it could just mean the JSON format changed, not that it is invalid/wrong
TEST(PointCollection, toGeoJSONWithMetadata) {
	PointCollection points = PointCollection(SpatioTemporalReference::unreferenced());

	points.local_md_value.addEmptyVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(2,3);
	points.addCoordinate(3,4);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1.000000,2.000000]]},"properties":{"test":5.100000}},{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[2.000000,3.000000],[3.000000,4.000000]]},"properties":{"test":2.100000}}]})";
	EXPECT_EQ(expected, points.toGeoJSON(true));
}

TEST(PointCollection, toCSV) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addEmptyVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);

	std::string expected = "feature,lon,lat,\"test\"\n0,1.000000,2.000000,5.100000\n1,1.000000,2.000000,2.100000\n1,2.000000,3.000000,2.100000\n";
	EXPECT_EQ(expected, points.toCSV());
}

TEST(PointCollection, toWKT) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addEmptyVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);

	std::string expected = "GEOMETRYCOLLECTION(POINT(1 2),MULTIPOINT((1 2),(2 3)))";
	EXPECT_EQ(expected, points.toWKT());
}

TEST(PointCollection, SimpletoARFF) {
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addEmptyVector("test");
	points.local_md_string.addEmptyVector("test2");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);
	points.local_md_string.set(0, "test2", "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);
	points.local_md_string.set(1, "test2", "TEST1234");

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
	PointCollection points(SpatioTemporalReference::unreferenced());
	points.local_md_value.addEmptyVector("test");
	points.local_md_string.addEmptyVector("test2");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);
	points.local_md_string.set(0, "test2", "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);
	points.local_md_string.set(1, "test2", "TEST1234");

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
	points.local_md_value.addEmptyVector("test");
	points.local_md_string.addEmptyVector("test2");

	points.addCoordinate(1,2);
	points.addCoordinate(2,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);
	points.local_md_string.set(0, "test2", "TEST123");

	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 2.1);
	points.local_md_string.set(1, "test2", "TEST1234");

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
	points.local_md_value.addEmptyVector("test");

	points.addCoordinate(1,2);
	points.finishFeature();
	points.local_md_value.set(0, "test", 5.1);

	points.addCoordinate(1,2);
	points.addCoordinate(2,3);
	points.finishFeature();
	points.local_md_value.set(1, "test", 4.1);

	points.addCoordinate(3,4);
	points.finishFeature();
	points.local_md_value.set(2, "test", 3.1);

	points.addCoordinate(5,6);
	points.addCoordinate(6,7);
	points.finishFeature();
	points.local_md_value.set(3, "test", 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(points.filter(keep), ArgumentException);

	keep.push_back(false);
	auto pointsFiltered = points.filter(keep);

	EXPECT_EQ(2, pointsFiltered->getFeatureCount());
	EXPECT_EQ(3, pointsFiltered->coordinates.size());
	EXPECT_EQ(2, pointsFiltered->local_md_value.getVector("test").size());
	EXPECT_DOUBLE_EQ(3.1, pointsFiltered->local_md_value.get(1, "test"));
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
