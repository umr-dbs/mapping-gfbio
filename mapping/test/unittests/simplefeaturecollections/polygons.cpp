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
	polygons.addCoordinate(1, 2);
	polygons.finishRing();
	polygons.finishPolygon();
	polygons.finishFeature();

	EXPECT_EQ(1, polygons.getFeatureCount());
	EXPECT_EQ(2, polygons.start_polygon.size());
	EXPECT_EQ(2, polygons.start_ring.size());
	EXPECT_EQ(3, polygons.coordinates.size());
}

TEST(PolygonCollection, Iterators) {
	PolygonCollection polygons(SpatioTemporalReference::unreferenced());
	for (int f=0;f<10000;f++) {
		for (int p=0;p<=f%3;p++) {
			for (int r=0;r<=f%4;r++) {
				for (int c=0;c<10;c++)
					polygons.addCoordinate(f+p+r, c);
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
