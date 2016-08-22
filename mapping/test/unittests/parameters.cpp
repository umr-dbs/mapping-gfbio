#include "util/configuration.h"

#include <gtest/gtest.h>

TEST(Parameters, getInt) {
	Parameters params;
	params["42"] = "42";
	// TODO: these three tests tell us the behaviour of stoi(). Not sure if that's the behaviour we want.
	params["43"] = " 43";
	params["44"] = "44 ";
	params["45"] = "45b";

	EXPECT_EQ(params.getInt("42"), 42);
	EXPECT_EQ(params.getInt("43"), 43);
	EXPECT_EQ(params.getInt("44"), 44);
	EXPECT_EQ(params.getInt("45"), 45);
}

TEST(Parameters, getBool) {
	Parameters params;
	params["yes"] = "yEs";
	params["true"] = "trUe";
	params["1"] = "1";
	params["no"] = "No";
	params["false"] = "faLSe";
	params["0"] = "0";

	EXPECT_EQ(params.getBool("yes"), true);
	EXPECT_EQ(params.getBool("true"), true);
	EXPECT_EQ(params.getBool("1"), true);
	EXPECT_EQ(params.getBool("no"), false);
	EXPECT_EQ(params.getBool("false"), false);
	EXPECT_EQ(params.getBool("0"), false);
}

TEST(Parameters, getPrefixedParameters) {
	Parameters params;
	params["test.a"] = "a";
	params["test.b"] = "b";
	params["test.c"] = "c";
	params["test."] = "should be ignored";
	params["other.a"] = "o.a";
	params["other.b"] = "o.b";
	params["other.c"] = "o.c";
	params["other.d"] = "o.d";
	params["a"] = "not a";

	auto prefixed = params.getPrefixedParameters("test.");
	EXPECT_EQ(prefixed.size(), 3);
	EXPECT_EQ(prefixed.get("a"), "a");
	EXPECT_EQ(prefixed.get("b"), "b");
	EXPECT_EQ(prefixed.get("c"), "c");
}

