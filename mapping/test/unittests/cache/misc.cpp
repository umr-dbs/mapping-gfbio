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

	printf("Test2 remainder:\n");
	for ( auto &r : res ) {
		printf("  %s\n", r.to_string().c_str() );
	}

	ASSERT_EQ( res.size(), 2 );

	ASSERT_EQ( res.at(0), Cube2(9,10,0,10));
	ASSERT_EQ( res.at(1), Cube2(0,9,9,10));
}

TEST(CubeTest, TestCube3) {
	Cube3 query( 0, 10, 0, 10, 0, 10 );
	auto res = query.dissect_by( Cube3(1,9,1,9,1,9) );

	printf("Test3 remainder:\n");
		for ( auto &r : res ) {
			printf("  %s\n", r.to_string().c_str() );
		}

	ASSERT_EQ( res.size(), 6 );

	ASSERT_EQ( res.at(0), Cube3(0,1,0,10,0,10));
	ASSERT_EQ( res.at(1), Cube3(9,10,0,10,0,10));

	ASSERT_EQ( res.at(2), Cube3(1,9,0,1,0,10));
	ASSERT_EQ( res.at(3), Cube3(1,9,9,10,0,10));

	ASSERT_EQ( res.at(4), Cube3(1,9,1,9,0,1));
	ASSERT_EQ( res.at(5), Cube3(1,9,1,9,9,10));
}

