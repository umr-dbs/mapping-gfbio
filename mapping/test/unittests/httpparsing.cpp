#include "services/httpparsing.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <sstream>

TEST(HTTPParsing, get) {
	HTTPService::Params params;

	EXPECT_EQ(setenv("REQUEST_METHOD", "GET", true), 0);
	EXPECT_EQ(setenv("QUERY_STRING", "param=one&PARAM=two&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F", true), 0);
	EXPECT_EQ(unsetenv("CONTENT_TYPE"), 0);
	EXPECT_EQ(unsetenv("CONTENT_LENGTH"), 0);

	EXPECT_NO_THROW(parseGetData(params));

	EXPECT_EQ(params.get("param", ""), "äöüß");
}


TEST(HTTPParsing, posturlencoded) {
	HTTPService::Params params;
	std::string content = "param=one&PARAM=two&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F";

	EXPECT_EQ(setenv("QUERY_STRING", "", true), 0);
	EXPECT_EQ(setenv("REQUEST_METHOD", "POST", true), 0);
	EXPECT_EQ(setenv("CONTENT_TYPE", "application/x-www-form-urlencoded", true), 0);
	EXPECT_EQ(setenv("CONTENT_LENGTH", std::to_string(content.length()).c_str(), true), 0);

	std::stringstream input;
	input << content;

	EXPECT_NO_THROW(parsePostData(params, input));

	//EXPECT_EQ(params.get("param", ""), "äöüß");
}


TEST(HTTPParsing, postformdata) {
	HTTPService::Params params;
	std::string content = "(TODO)";

	EXPECT_EQ(setenv("QUERY_STRING", "", true), 0);
	EXPECT_EQ(setenv("REQUEST_METHOD", "POST", true), 0);
	EXPECT_EQ(setenv("CONTENT_TYPE", "multipart/form-data", true), 0);
	EXPECT_EQ(setenv("CONTENT_LENGTH", std::to_string(content.length()).c_str(), true), 0);

	std::stringstream input;
	input << content;

	EXPECT_NO_THROW(parsePostData(params, input));

	//EXPECT_EQ(params.get("param", ""), "äöüß");
}
