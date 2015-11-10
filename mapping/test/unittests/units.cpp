#include <gtest/gtest.h>
#include <iostream>
#include "datatypes/unit.h"
#include "util/exceptions.h"


static std::string round_trip(const std::string &json) {
	Unit u(json);
	auto json2 = u.toJson();
	Unit u2(json2);
	auto json3 = u2.toJson();
	if (json2 != json3)
		throw ArgumentException("Unit::toJson() is broken");
	return json2;
}

TEST(Unit, parsing) {

	/*
	 * First, we check a bunch of invalid unit strings to see if parsing fails properly
	 */
// just a shorthand to make the following list more readable
#define F(str) EXPECT_THROW(Unit u(str), ArgumentException)

	F("");
	F("{\"measurement\":\"Temperature\"}");
	F("{\"unit\":\"C\"}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\"}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"C\",\"classes\":1}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\",\"classes\":1}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\",\"classes\":[1]}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\",\"classes\":1}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\",\"classes\":{1:\"One\"}, \"interpolation\":\"continuous\"}");
	F("{\"measurement\":\"Temperature\", \"unit\":\"classification\", \"interpolation\":\"NeitherDiscreteNorContinuous\"}");

	/*
	 * Now it's time to check valid units to see if parsing works properly
	 */
	{
		Unit u(round_trip("{\"measurement\":\"Temperature\",\"unit\":\"C\"}"));
		EXPECT_EQ(u.getMeasurement(), "temperature");
		EXPECT_EQ(u.getUnit(), "c");
	}
	{
		Unit u(round_trip("{\"measurement\":\"Temperature\",\"unit\":\"Classification\",\"classes\":{\"1\":\"too cold\",\"2\":\"too hot\"}}"));
		EXPECT_EQ(u.getMeasurement(), "temperature");
		EXPECT_EQ(u.isClassification(), true);
	}
	{
		Unit u(round_trip("{\"measurement\":\"Temperature\",\"unit\":\"C\", \"min\": 0, \"max\": 42}"));
		EXPECT_EQ(u.getMeasurement(), "temperature");
		EXPECT_EQ(u.getMin(), 0.0);
		EXPECT_EQ(u.getMax(), 42.0);
	}
	{
		Unit u(round_trip("{\"measurement\":\"Temperature\",\"unit\":\"C\", \"interpolation\": \"continuous\"}"));
		EXPECT_EQ(u.getMeasurement(), "temperature");
		EXPECT_EQ(u.isContinuous(), true);
	}
}
