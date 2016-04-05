#include <gtest/gtest.h>
#include "util/sha1.h"


static void testSHA1(const std::string &key, const std::string &hash) {
	{
		SHA1 sha1;
		sha1.addBytes(key.c_str(), key.size());
		auto res = sha1.digest().asHex();

		EXPECT_EQ(res, hash);
	}

	{
		SHA1 sha1;
		for (size_t i=0;i<key.size();i+=7) {
			size_t remaining = std::min(key.size()-i, (size_t) 7);
			sha1.addBytes(&(key.c_str()[i]), remaining);
		}
		auto res = sha1.digest().asHex();

		EXPECT_EQ(res, hash);
	}
}

TEST(SHA1, hash) {
	testSHA1("", "da39a3ee5e6b4b0d3255bfef95601890afd80709");
	testSHA1("SHA1", "e1744a525099d9a53c0460ef9cb7ab0e4c4fc939");
	testSHA1("unpictured", "2d36a990a745a7ffd8f6361f598c414e366944be");
	testSHA1("undiplomatically", "e9b4c26b627b7303772f8f35426d788dde050ded");
	testSHA1("postbag", "695d86c07a3ba3a38162e3f0ab8570903a20696a");
	testSHA1("constrictive", "e38de31dfdae203c8f2ad150ebf1959600b15dc8");
	testSHA1("uniformed", "7f247ae702afebb1457833453041f2f1575920ce");
}
