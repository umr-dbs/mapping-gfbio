#include "services/httpparsing.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <sstream>


void parseCGIEnvironment(
	HTTPService::Params &params,
	const std::string &method,
	const std::string &url,
	const std::string &query_string,
	const std::string &postmethod = "",
	const std::string &postdata = "") {

	std::stringstream input;

	EXPECT_EQ(setenv("REQUEST_METHOD", method.c_str(), true), 0);
	EXPECT_EQ(setenv("QUERY_STRING", query_string.c_str(), true), 0);
	std::string request_uri = url;
	if (query_string != "")
		request_uri += "?" + query_string;
	EXPECT_EQ(setenv("REQUEST_URI", request_uri.c_str(), true), 0);
	if (method == "GET") {
		EXPECT_EQ(unsetenv("CONTENT_TYPE"), 0);
		EXPECT_EQ(unsetenv("CONTENT_LENGTH"), 0);
	}
	else if (method == "POST") {
		EXPECT_EQ(setenv("CONTENT_TYPE", postmethod.c_str(), true), 0);
		EXPECT_EQ(setenv("CONTENT_LENGTH", std::to_string(postdata.length()).c_str(), true), 0);

		input << postdata;
	}
	else
		FAIL();

	EXPECT_NO_THROW(parseGetData(params));
	EXPECT_NO_THROW(parsePostData(params, input));
}


// Test a parameter that appears multiple times and has special characters
TEST(HTTPParsing, getrepeated) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "PARAM=one&PARAM=two&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F");

	EXPECT_EQ(params.get("param", ""), "äöüß");
}

// Test a parameter that has no value.
TEST(HTTPParsing, getnovalue) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "flag1&flag2&flag3&value1=3&value2=4");

	EXPECT_EQ(params.get("flag1", "-"), "");
	EXPECT_EQ(params.get("flag2", "-"), "");
	EXPECT_EQ(params.get("flag3", "-"), "");
	EXPECT_EQ(params.get("value1", ""), "3");
	EXPECT_EQ(params.get("value2", ""), "4");
}

// Test an empty query string
TEST(HTTPParsing, emptyget) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "");

	EXPECT_EQ(params.size(), 0);
}

// Test urlencoded postdata
TEST(HTTPParsing, posturlencoded) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "",
		"application/x-www-form-urlencoded", "flag1&param=one&PARAM=two&flag2&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F"
	);

	EXPECT_EQ(params.get("flag1", "-"), "");
	EXPECT_EQ(params.get("flag2", "-"), "");
	EXPECT_EQ(params.get("param", ""), "äöüß");
}

// Test multipart postdata
TEST(HTTPParsing, postformdata) {
	HTTPService::Params params;
	// TODO: Not implemented. Needs multipart first.
	parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "",
		"multipart/form-data", "(TODO)"
	);

	//EXPECT_EQ(params.get("param", ""), "äöüß");
}

// Test weird query string formats
TEST(HTTPParsing, testquerystringspecialchars) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "p1&p2=1=2=%C3%A4%C3%B6%C3%BC%C3%9F&p3=&p4&&p5&?????&&&====&=&???&&p6==?");

	EXPECT_EQ(params.get("p1", "-"), "");
	EXPECT_EQ(params.get("p2", ""), "1=2=äöüß");
	EXPECT_EQ(params.get("p3", "-"), "");
	EXPECT_EQ(params.get("p4", "-"), "");
	EXPECT_EQ(params.get("p5", "-"), "");
	EXPECT_EQ(params.get("p6", ""), "=?");
	EXPECT_EQ(params.size(), 8); // Also interprets '?????' and '???' as keys.
}

// Test weird query string formats
TEST(HTTPParsing, testparameteroverwrites) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "p1=a&p1=b&p1=c&p1");

	EXPECT_EQ(params.get("p1", "-"), "");
}

// Test illegal percent encoding
TEST(HTTPParsing, illegalpercentencoding) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla", "p1=%22%ZZ%5F");

	EXPECT_EQ(params.get("p1", "-"), "\"%ZZ_");
}


