#include <gtest/gtest.h>
#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <vector>

#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "raster/opencl.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"

TEST(LineCollection, GeosGeomConversion) {
	LineCollection lines(SpatioTemporalReference::unreferenced());
	std::string wkt = "GEOMETRYCOLLECTION(MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10)),LINESTRING (30 10, 10 30, 40 40))";
	auto lineCollection = WKBUtil::readLineCollection(wkt);

	EXPECT_EQ(2, lineCollection->getFeatureCount());
	EXPECT_EQ(10, lineCollection->coordinates.size());

	auto geometry = GeosGeomUtil::createGeosLineCollection(*lineCollection.get());

	EXPECT_EQ(2, geometry->getNumGeometries());
}


TEST(LineCollection, Iterators) {
	LineCollection lines(SpatioTemporalReference::unreferenced());
	for (int f=0;f<10000;f++) {
		for (int l=0;l<=f%3;l++) {
			for (int c=0;c<10;c++)
				lines.addCoordinate(f+l, c);
			lines.finishLine();
		}
		lines.finishFeature();
	}

	double res_loop = 0;
	auto featurecount = lines.getFeatureCount();
	for (size_t i=0;i<featurecount;i++) {
		auto startf = lines.start_feature[i];
		auto endf = lines.start_feature[i+1];
		for (size_t l = startf; l < endf; l++) {
			auto startl = lines.start_line[l];
			auto endl = lines.start_line[l+1];
			for (size_t c = startl; c < endl; c++) {
				res_loop += lines.coordinates[c].x;
			}
		}
	}

	double res_iter = 0;
	for (auto feature : lines) {
		for (auto line : feature) {
			for (auto & c : line) {
				res_iter += c.x;
			}
		}
	}

	double res_citer = 0;
	const LineCollection &clines = lines;
	for (auto feature : clines) {
		for (auto line : feature) {
			for (auto & c : line) {
				res_citer += c.x;
			}
		}
	}

	EXPECT_EQ(res_loop, res_iter);
	EXPECT_EQ(res_loop, res_citer);
}

TEST(LineCollection, directReferenceAccess){
	LineCollection lines(SpatioTemporalReference::unreferenced());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();

	EXPECT_EQ(2, lines.getFeatureReference(0).getLineReference(0).size());
	EXPECT_EQ(2, lines.getFeatureReference(1).getLineReference(1).size());
}

TEST(LineCollection, filter) {
	LineCollection lines(SpatioTemporalReference::unreferenced());
	lines.local_md_value.addEmptyVector("test");

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(0, "test", 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(1, "test", 4.1);

	lines.addCoordinate(7,8);
	lines.addCoordinate(6,5);
	lines.addCoordinate(6,2);
	lines.finishLine();
	lines.addCoordinate(1,4);
	lines.addCoordinate(12,6);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(2, "test", 3.1);

	lines.addCoordinate(5,6);
	lines.addCoordinate(6,7);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(3, "test", 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(lines.filter(keep), ArgumentException);

	keep.push_back(false);
	auto linesFiltered = lines.filter(keep);

	EXPECT_EQ(2, linesFiltered->getFeatureCount());
	EXPECT_EQ(9, linesFiltered->coordinates.size());
	EXPECT_EQ(2, linesFiltered->local_md_value.getVector("test").size());
	EXPECT_DOUBLE_EQ(3.1, linesFiltered->local_md_value.get(1, "test"));
}

TEST(LineCollection, toWKT){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	lines.local_md_value.addEmptyVector("test");

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(0, "test", 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_value.set(1, "test", 4.1);

	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 2,1 3),MULTILINESTRING((1 2,2 3),(2 4,5 6)))";
	EXPECT_EQ(wkt, lines.toWKT());
}

TEST(LineCollection, toARFF){
	//TODO: test missing metadata value
	LineCollection lines(SpatioTemporalReference::unreferenced());
	lines.local_md_string.addEmptyVector("test");
	lines.local_md_value.addEmptyVector("test2");

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_string.set(0, "test", "test");
	lines.local_md_value.set(0, "test2", 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	lines.local_md_string.set(1, "test", "test2");
	lines.local_md_value.set(1, "test2", 4.1);

	lines.addDefaultTimestamps();

	std::string expected = "@RELATION export\n"
			"\n"
			"@ATTRIBUTE wkt STRING\n"
			"@ATTRIBUTE time_start DATE\n"
			"@ATTRIBUTE time_end DATE\n"
			"@ATTRIBUTE test STRING\n"
			"@ATTRIBUTE test2 NUMERIC\n"
			"\n"
			"@DATA\n"
			"\"LINESTRING(1 2,1 3)\",\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"test\",5.1\n"
			"\"MULTILINESTRING((1 2,2 3),(2 4,5 6))\",\"1970-01-01T00:00:00\",\"1970-01-01T00:00:00\",\"test2\",4.1\n";

	EXPECT_EQ(expected, lines.toARFF());
}

TEST(LineCollection, calculateMBR){
	LineCollection lines(SpatioTemporalReference::unreferenced());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.addCoordinate(2,5);
	lines.finishLine();
	lines.finishFeature();

	lines.addCoordinate(-2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.addCoordinate(1,-4);
	lines.addCoordinate(3,-6);
	lines.finishLine();
	lines.finishFeature();

	auto mbr = lines.mbr();
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);

	mbr = lines.featureMBR(0);
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(1, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(3, mbr.y2);

	mbr = lines.featureMBR(1);
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(2, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(5, mbr.y2);

	mbr = lines.featureMBR(2);
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);


	mbr = lines.lineMBR(2,0);
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(4, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);

	mbr = lines.lineMBR(2,1);
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(3, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(-4, mbr.y2);
}
