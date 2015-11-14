/*
 * aux.cpp
 *
 *  Created on: 14.11.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include <vector>
#include "cache/priv/cube.h"


class Cube2 : public Cube<2> {
public:
	Cube2( double x1, double x2, double y1, double y2 ) {
		set_dimension( 0, x1, x2 );
		set_dimension( 1, y1, y2 );
	};
};

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

	if ( res.size() != 2 )
		throw std::runtime_error("test2: should have 2 remainders");

	ASSERT_DOUBLE_EQ( res.at(0).get_dimension(0).a, 9 );
	ASSERT_DOUBLE_EQ( res.at(0).get_dimension(0).b, 10 );
	ASSERT_DOUBLE_EQ( res.at(0).get_dimension(1).a, 0 );
	ASSERT_DOUBLE_EQ( res.at(0).get_dimension(1).b, 10 );


	ASSERT_DOUBLE_EQ( res.at(1).get_dimension(0).a, 0 );
	ASSERT_DOUBLE_EQ( res.at(1).get_dimension(0).b, 9 );
	ASSERT_DOUBLE_EQ( res.at(1).get_dimension(1).a, 9 );
	ASSERT_DOUBLE_EQ( res.at(1).get_dimension(1).b, 10 );
}



