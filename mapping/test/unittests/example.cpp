#include <gtest/gtest.h>

TEST(ExampleTestCase, ExampleTest) {
	int i = 5;
	ASSERT_EQ(i, 5);
}

class ExampleTestCaseWithFixture: public ::testing::Test {
protected:
	ExampleTestCaseWithFixture() : sharedResource(1) {
		sharedResource = 42;
	}

	// per-test set-up
	virtual void SetUp() {
		sharedResource = 0;
	}

	// per-test tear-down
	virtual void TearDown() {
		sharedResource = -1;
	}

	// Shared resources
	int sharedResource;
};

TEST_F(ExampleTestCaseWithFixture, ExampleTest) {
	ASSERT_EQ(sharedResource, 0);
}
