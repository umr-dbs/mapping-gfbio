/*
 * aux.cpp
 *
 *  Created on: 14.11.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include <vector>
#include "cache/priv/cube.h"


TEST(CubeTest,TestCube2_1) {

	Cube2 query( 5,10,5,10 );
	std::vector<Cube2> entries;

	auto res = query.dissect_by( Cube2(4,11,4,11) );
	ASSERT_TRUE( res.empty() );
}

TEST(CubeTest, TestCube2_2) {
	Cube2 query( 0,10,0,10 );
	auto res = query.dissect_by(Cube2(0,9,0,9));

	ASSERT_EQ( res.size(), 2 );

	ASSERT_EQ( res.at(0), Cube2(9,10,0,10));
	ASSERT_EQ( res.at(1), Cube2(0,9,9,10));
}

TEST(CubeTest, TestCube3) {
	Cube3 query( 0, 10, 0, 10, 0, 10 );
	auto res = query.dissect_by( Cube3(1,9,1,9,1,9) );

	ASSERT_EQ( res.size(), 6 );

	ASSERT_EQ( res.at(0), Cube3(0,1,0,10,0,10));
	ASSERT_EQ( res.at(1), Cube3(9,10,0,10,0,10));

	ASSERT_EQ( res.at(2), Cube3(1,9,0,1,0,10));
	ASSERT_EQ( res.at(3), Cube3(1,9,9,10,0,10));

	ASSERT_EQ( res.at(4), Cube3(1,9,1,9,0,1));
	ASSERT_EQ( res.at(5), Cube3(1,9,1,9,9,10));
}

//TEST(Misc,StdDev) {
//	std::vector<double> vals = {0.75,1.0,1.25};
//
//	double sum = 0;
//	double sqsum = 0;
//
//	for (auto &v : vals) {
//		sum += v;
//		sqsum += v*v;
//	}
//
//	double avg = sum / vals.size();
//
//	double stddev1 = 0;
//	if ( vals.size() > 1 )
//	stddev1 = std::sqrt( std::max(0.0,
//			// Incremental calculation of std-dev
//			(sqsum - (sum*sum) / vals.size()) / (vals.size()) ));
//
//	// Orig way to do it
//	double stddev2 = 0;
//	for ( auto &v : vals ) {
//		stddev2 += (v-avg)*(v-avg);
//	}
//	stddev2 = std::sqrt(stddev2/vals.size());
//
//	printf("STDDEV: %f\n", stddev1);
//
//	EXPECT_DOUBLE_EQ(1,avg);
//	EXPECT_DOUBLE_EQ(stddev2,stddev1);
//}
