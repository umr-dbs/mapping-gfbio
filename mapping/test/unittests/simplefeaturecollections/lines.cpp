#include <gtest/gtest.h>
#include <iostream>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include <vector>
#include "util/binarystream.h"

#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "raster/opencl.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "datatypes/simplefeaturecollections/geosgeomutil.h"
#include "test/unittests/simplefeaturecollections/util.h"

std::unique_ptr<LineCollection> createLinesWithAttributesAndTime(){
	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 1, 2 3, 14 8), LINESTRING(14 10, 45 7, 26 3), MULTILINESTRING((8 6, 8 9, 88 99), (47 11, 47 8, 99 3)), LINESTRING(68 59, 11 15, 77 44, 84 13), LINESTRING(78 65, 32 14, 36 63))";
	auto lines = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());
	lines->time = {TimeInterval(2, 4), TimeInterval(4, 8), TimeInterval(8, 16), TimeInterval (16, 32), TimeInterval(16, 32), TimeInterval(32, 64)};

	lines->setTimeStamps({2, 4,  8, 16, 32}, {4, 8, 16, 32, 64});

	lines->global_attributes.setTextual("info", "1234");
	lines->global_attributes.setNumeric("index", 42);

	lines->feature_attributes.addNumericAttribute("value", Unit::unknown(), {0.0, 1.1, 2.2, 3.3, 4.4});
	lines->feature_attributes.addTextualAttribute("label", Unit::unknown(), {"l0", "l1", "l2", "l3", "l4"});

	EXPECT_NO_THROW(lines->validate());

	return lines;
}

TEST(LineCollection, GeosGeomConversion) {
	LineCollection lines(SpatioTemporalReference::unreferenced());
	std::string wkt = "GEOMETRYCOLLECTION(MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10)),LINESTRING (30 10, 10 30, 40 40))";
	auto lineCollection = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(2, lineCollection->getFeatureCount());
	EXPECT_EQ(10, lineCollection->coordinates.size());

	auto geometry = GeosGeomUtil::createGeosLineCollection(*lineCollection.get());

	EXPECT_EQ(2, geometry->getNumGeometries());
}

TEST(LineCollection, Invalid){
	LineCollection lines(SpatioTemporalReference::unreferenced());

	EXPECT_THROW(lines.finishLine(), FeatureException);
	EXPECT_THROW(lines.finishFeature(), FeatureException);
	EXPECT_NO_THROW(lines.validate());


	lines.addCoordinate(1, 2);
	EXPECT_THROW(lines.finishLine(), FeatureException);
	lines.addCoordinate(2, 2);
	lines.finishLine();
	EXPECT_THROW(lines.validate(), FeatureException);
	lines.finishFeature();
	EXPECT_NO_THROW(lines.validate());
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

TEST(LineCollection, iterateEmptyCollection){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	size_t foo = 0;
	for(auto feature : lines){
		for(auto line : feature){
			for(auto& coordinate : line){
				foo += coordinate.x;
			}
		}
	}
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
	lines.addCoordinate(1,6);
	lines.finishLine();
	lines.finishFeature();

	EXPECT_EQ(2, lines.getFeatureReference(0).getLineReference(0).size());
	EXPECT_EQ(3, lines.getFeatureReference(1).getLineReference(1).size());
}

TEST(LineCollection, filter) {
	LineCollection lines(SpatioTemporalReference::unreferenced());
	auto &test = lines.feature_attributes.addNumericAttribute("test", Unit::unknown());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	test.set(0, 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(1, 4.1);

	lines.addCoordinate(7,8);
	lines.addCoordinate(6,5);
	lines.addCoordinate(6,2);
	lines.finishLine();
	lines.addCoordinate(1,4);
	lines.addCoordinate(12,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(2, 3.1);

	lines.addCoordinate(5,6);
	lines.addCoordinate(6,7);
	lines.finishLine();
	lines.finishFeature();
	test.set(3, 2.1);

	std::vector<bool> keep {false, true, true};

	EXPECT_THROW(lines.filter(keep), ArgumentException);

	keep.push_back(false);
	auto linesFiltered = lines.filter(keep);

	EXPECT_NO_THROW(linesFiltered->validate());
	EXPECT_EQ(2, linesFiltered->getFeatureCount());
	EXPECT_EQ(9, linesFiltered->coordinates.size());
	EXPECT_DOUBLE_EQ(3.1, linesFiltered->feature_attributes.numeric("test").get(1));
}

TEST(LineCollection, toGeoJSON){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	auto &test = lines.feature_attributes.addNumericAttribute("test", Unit::unknown());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	test.set(0, 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(1, 4.1);

	lines.addDefaultTimestamps();

	std::string expected = "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:1\"}},\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"LineString\",\"coordinates\":[[1.000000,2.000000],[1.000000,3.000000]]}},{\"type\":\"Feature\",\"geometry\":{\"type\":\"MultiLineString\",\"coordinates\":[[[1.000000,2.000000],[2.000000,3.000000]],[[2.000000,4.000000],[5.000000,6.000000]]]}}]}";

	EXPECT_EQ(expected, lines.toGeoJSON(false));
}

TEST(LineCollection, toGeoJSONEmptyCollection){
	LineCollection lines(SpatioTemporalReference::unreferenced());

	std::string expected = "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:1\"}},\"features\":[]}";

	EXPECT_EQ(expected, lines.toGeoJSON(false));
}

TEST(LineCollection, toGeoJSONMetadata){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	auto &test = lines.feature_attributes.addTextualAttribute("test", Unit::unknown());
	auto &test2 = lines.feature_attributes.addNumericAttribute("test2", Unit::unknown());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	test.set(0, "test");
	test2.set(0, 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(1, "test123");
	test2.set(1, 4.1);

	lines.addDefaultTimestamps(0,1);

	std::string expected = R"({"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:1"}},"features":[{"type":"Feature","geometry":{"type":"LineString","coordinates":[[1.000000,2.000000],[1.000000,3.000000]]},"properties":{"test":"test","test2":5.100000,"time_start":0.000000,"time_end":1.000000}},{"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[[[1.000000,2.000000],[2.000000,3.000000]],[[2.000000,4.000000],[5.000000,6.000000]]]},"properties":{"test":"test123","test2":4.100000,"time_start":0.000000,"time_end":1.000000}}]})";

	EXPECT_EQ(expected, lines.toGeoJSON(true));
}


TEST(LineCollection, toWKT){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	auto &test = lines.feature_attributes.addNumericAttribute("test", Unit::unknown());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	test.set(0, 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(1, 4.1);

	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 2,1 3),MULTILINESTRING((1 2,2 3),(2 4,5 6)))";
	EXPECT_EQ(wkt, lines.toWKT());
}

TEST(LineCollection, toARFF){
	//TODO: test missing metadata value
	TemporalReference tref(TIMETYPE_UNIX);
	SpatioTemporalReference stref(SpatialReference::unreferenced(), tref);
	LineCollection lines(stref);//is there a better way to initialize?

	auto &test = lines.feature_attributes.addTextualAttribute("test", Unit::unknown());
	auto &test2 = lines.feature_attributes.addNumericAttribute("test2", Unit::unknown());

	lines.addCoordinate(1,2);
	lines.addCoordinate(1,3);
	lines.finishLine();
	lines.finishFeature();
	test.set(0, "test");
	test2.set(0, 5.1);

	lines.addCoordinate(1,2);
	lines.addCoordinate(2,3);
	lines.finishLine();
	lines.addCoordinate(2,4);
	lines.addCoordinate(5,6);
	lines.finishLine();
	lines.finishFeature();
	test.set(1, "test2");
	test2.set(1, 4.1);

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

	auto mbr = lines.getCollectionMBR();
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);

	mbr = lines.getFeatureReference(0).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(1, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(3, mbr.y2);

	mbr = lines.getFeatureReference(1).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(2, mbr.x2);
	EXPECT_DOUBLE_EQ(2, mbr.y1);
	EXPECT_DOUBLE_EQ(5, mbr.y2);

	mbr = lines.getFeatureReference(2).getMBR();
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);


	mbr = lines.getFeatureReference(2).getLineReference(0).getMBR();
	EXPECT_DOUBLE_EQ(-2, mbr.x1);
	EXPECT_DOUBLE_EQ(5, mbr.x2);
	EXPECT_DOUBLE_EQ(4, mbr.y1);
	EXPECT_DOUBLE_EQ(6, mbr.y2);

	mbr = lines.getFeatureReference(2).getLineReference(1).getMBR();
	EXPECT_DOUBLE_EQ(1, mbr.x1);
	EXPECT_DOUBLE_EQ(3, mbr.x2);
	EXPECT_DOUBLE_EQ(-6, mbr.y1);
	EXPECT_DOUBLE_EQ(-4, mbr.y2);
}

TEST(LineCollection, WKTImport){
	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 2, 3 4, 5 6))";
	auto lines = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, lines->getFeatureCount());
	EXPECT_EQ(1, lines->coordinates[0].x);
	EXPECT_EQ(2, lines->coordinates[0].y);
	EXPECT_EQ(5, lines->coordinates[2].x);
	EXPECT_EQ(6, lines->coordinates[2].y);
}

TEST(LineCollection, WKTImportMulti){
	std::string wkt = "GEOMETRYCOLLECTION(MULTILINESTRING((1 2, 3 4, 5 6), (7 8, 9 10, 11 12, 13 14)))";
	auto lines = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(1, lines->getFeatureCount());
	EXPECT_EQ(1, lines->coordinates[0].x);
	EXPECT_EQ(2, lines->coordinates[0].y);
	EXPECT_EQ(13, lines->coordinates[6].x);
	EXPECT_EQ(14, lines->coordinates[6].y);
}

TEST(LineCollection, WKTImportMixed){
	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 2, 3 4, 5 6), MULTILINESTRING((1 2, 3 4, 5 6), (7 8, 9 10, 11 12, 13 14)))";
	auto lines = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());

	EXPECT_EQ(2, lines->getFeatureCount());
	EXPECT_EQ(1, lines->coordinates[0].x);
	EXPECT_EQ(2, lines->coordinates[0].y);
	EXPECT_EQ(5, lines->coordinates[2].x);
	EXPECT_EQ(6, lines->coordinates[2].y);

	EXPECT_EQ(1, lines->start_feature[1]);
	EXPECT_EQ(3, lines->start_line[1]);

	EXPECT_EQ(13, lines->coordinates[9].x);
	EXPECT_EQ(14, lines->coordinates[9].y);
}

TEST(LineCollection, WKTAddSingleFeature){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	lines.addCoordinate(1, 2);
	lines.addCoordinate(2, 3);
	lines.addCoordinate(3, 4);
	lines.finishLine();
	lines.finishFeature();
	std::string wkt = "LINESTRING(3 4, 5 5, 6 7)";
	WKBUtil::addFeatureToCollection(lines, wkt);

	EXPECT_EQ(2, lines.getFeatureCount());
	EXPECT_EQ(1, lines.coordinates[0].x);
	EXPECT_EQ(2, lines.coordinates[0].y);
	EXPECT_EQ(6, lines.coordinates[5].x);
	EXPECT_EQ(7, lines.coordinates[5].y);
}

TEST(LineCollection, WKTAddMultiFeature){
	LineCollection lines(SpatioTemporalReference::unreferenced());
	lines.addCoordinate(1, 2);
	lines.addCoordinate(2, 3);
	lines.addCoordinate(3, 4);
	lines.finishLine();
	lines.finishFeature();
	std::string wkt = "MULTILINESTRING((3 4, 5 6, 8 8), (9 9, 5 2, 1 1))";
	WKBUtil::addFeatureToCollection(lines, wkt);

	EXPECT_EQ(2, lines.getFeatureCount());
	EXPECT_EQ(2, lines.getFeatureReference(1).size());
	EXPECT_EQ(1, lines.coordinates[0].x);
	EXPECT_EQ(2, lines.coordinates[0].y);
	EXPECT_EQ(2, lines.coordinates[1].x);
	EXPECT_EQ(3, lines.coordinates[1].y);

	EXPECT_EQ(1, lines.start_feature[1]);
	EXPECT_EQ(3, lines.start_line[1]);

	EXPECT_EQ(1, lines.coordinates[8].x);
	EXPECT_EQ(1, lines.coordinates[8].y);
}

TEST(LineCollection, WKTAddFeatureFail){
	auto lines = createLinesWithAttributesAndTime();
	std::string wkt = "POINT(3 foo)";
	EXPECT_ANY_THROW(WKBUtil::addFeatureToCollection(*lines, wkt));

	auto result = createLinesWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *lines);
}


std::unique_ptr<LineCollection> createLinesForSTRefFilter(){
	auto stref = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 100, 100),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 100));

	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 2, 5 5, 8 9), LINESTRING(9 8, 4 4, 1 9), LINESTRING(6 6, 11 12, 20 30), LINESTRING(0 10, 10 10, 10 0), LINESTRING(33 40, 49 17, 88 65), MULTILINESTRING((9 8, 8 7, 7 6), (0 1, 1 2, 2 3)))";
	auto lines = WKBUtil::readLineCollection(wkt, stref);

	EXPECT_NO_THROW(lines->validate());

	return lines;
}

TEST(LineCollection, filterBySTRefIntersection){
	auto lines = createLinesForSTRefFilter();

	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));

	auto filtered = lines->filterBySpatioTemporalReferenceIntersection(filter);

	std::vector<bool> keep = {true, true, true, true, false, true};
	auto expected = lines->filter(keep);
	expected->replaceSTRef(filter);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(LineCollection, filterBySTRefIntersectionWithTime){
	auto lines = createLinesForSTRefFilter();
	lines->setTimeStamps({1,  20,  5  ,3,  4, 11}, {9,  22,  30,  4, 88, 12});

	EXPECT_NO_THROW(lines->validate());

	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));

	auto filtered = lines->filterBySpatioTemporalReferenceIntersection(filter);

	std::vector<bool> keep = {true, false, true, true, false, false};
	auto expected = lines->filter(keep);
	expected->replaceSTRef(filter);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(LineCollection, filterBySTRefIntersectionInPlace){
	auto lines = createLinesForSTRefFilter();

	auto filter = SpatioTemporalReference(SpatialReference(EPSG_UNKNOWN, 0, 0, 10, 10),
					TemporalReference(TIMETYPE_UNKNOWN, 0, 10));

	std::vector<bool> keep = {true, true, true, true, false, true};
	auto expected = lines->filter(keep);
	expected->replaceSTRef(filter);

	lines->filterBySpatioTemporalReferenceIntersectionInPlace(filter);

	CollectionTestUtil::checkEquality(*expected, *lines);
}

TEST(LineCollection, filterInPlace){
	auto lines = createLinesWithAttributesAndTime();
	std::vector<bool> keep = {true, false, true, false, true};

	auto expected = lines->filter(keep);

	lines->filterInPlace(keep);

	CollectionTestUtil::checkEquality(*expected, *lines);
}

TEST(LineCollection, filterByPredicate){
	auto lines = createLinesWithAttributesAndTime();

	auto filtered = lines->filter([](const LineCollection &c, size_t feature) {
		return c.time[feature].t1 >= 16;
	});

	std::vector<bool> keep({false, false, false, true, true});
	auto expected = lines->filter(keep);

	CollectionTestUtil::checkEquality(*expected, *filtered);
}

TEST(LineCollection, filterByPredicateInPlace){
	auto lines = createLinesWithAttributesAndTime();

	std::vector<bool> keep({false, false, false, true, true});
	auto expected = lines->filter(keep);

	lines->filterInPlace([](LineCollection &c, size_t feature) {
		return c.time[feature].t1 >= 16;
	});

	CollectionTestUtil::checkEquality(*expected, *lines);
}

TEST(LineCollection, StreamSerialization){
	LineCollection lines(SpatioTemporalReference::unreferenced());

	//TODO: attributes, time array

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

	auto stream = BinaryStream::makePipe();
	BinaryWriteBuffer wb;
	wb.write(lines);
	stream.write(wb);

	BinaryReadBuffer rb;
	stream.read(rb);
	LineCollection lines2(rb);

	CollectionTestUtil::checkEquality(lines, lines2);
}

TEST(LineCollection, removeLastFeature){
	auto lines = createLinesWithAttributesAndTime();

	lines->removeLastFeature();
	lines->validate();

	std::string wkt = "GEOMETRYCOLLECTION(LINESTRING(1 1, 2 3, 14 8), LINESTRING(14 10, 45 7, 26 3), MULTILINESTRING((8 6, 8 9, 88 99), (47 11, 47 8, 99 3)), LINESTRING(68 59, 11 15, 77 44, 84 13))";
	auto result = WKBUtil::readLineCollection(wkt, SpatioTemporalReference::unreferenced());
	result->setTimeStamps({2, 4,  8, 16}, {4, 8, 16, 32});

	result->global_attributes.setTextual("info", "1234");
	result->global_attributes.setNumeric("index", 42);

	result->feature_attributes.addNumericAttribute("value", Unit::unknown(), {0.0, 1.1, 2.2, 3.3});
	result->feature_attributes.addTextualAttribute("label", Unit::unknown(), {"l0", "l1", "l2", "l3"});

	result->validate();

	CollectionTestUtil::checkEquality(*result, *lines);
}

TEST(LineCollection, removeLastFeatureUnfinishedLine){
	auto lines = createLinesWithAttributesAndTime();

	lines->addCoordinate(2,3);
	lines->addCoordinate(3,3);
	lines->addCoordinate(5,3);
	lines->feature_attributes.textual("label").set(lines->getFeatureCount(), "fail");

	lines->removeLastFeature();
	lines->validate();

	auto result = createLinesWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *lines);
}

TEST(LineCollection, removeLastFeatureUnfinishedFeature){
	auto lines = createLinesWithAttributesAndTime();

	lines->addCoordinate(2,3);
	lines->addCoordinate(3,3);
	lines->addCoordinate(5,3);
	lines->finishLine();
	lines->feature_attributes.textual("label").set(lines->getFeatureCount(), "fail");

	lines->removeLastFeature();
	lines->validate();

	auto result = createLinesWithAttributesAndTime();

	CollectionTestUtil::checkEquality(*result, *lines);
}
