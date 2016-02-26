#include "services/httpparsing.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <sstream>
#include <util/exceptions.h>
#include <algorithm>

void parseCGIEnvironment(HTTPService::Params &params, std::string method,
		const std::string &url, const std::string &query_string,
		const std::string &postmethod = "", const std::string &postdata = "") {

	std::stringstream input;

	EXPECT_EQ(setenv("REQUEST_METHOD", method.c_str(), true), 0);
	EXPECT_EQ(setenv("QUERY_STRING", query_string.c_str(), true), 0);

	// force method upper-case to allow case-sensitivity checks in the parse*Data methods.
	std::transform(method.begin(), method.end(), method.begin(), ::toupper);

	std::string request_uri = url;
	if (query_string != "")
		request_uri += "?" + query_string;
	EXPECT_EQ(setenv("REQUEST_URI", request_uri.c_str(), true), 0);
	if (method == "GET") {
		EXPECT_EQ(unsetenv("CONTENT_TYPE"), 0);
		EXPECT_EQ(unsetenv("CONTENT_LENGTH"), 0);
	} else if (method == "POST") {
		EXPECT_EQ(setenv("CONTENT_TYPE", postmethod.c_str(), true), 0);
		EXPECT_EQ(
				setenv("CONTENT_LENGTH",
						std::to_string(postdata.length()).c_str(), true), 0);

		input << postdata;
	} else
		FAIL();

	parseGetData(params);
	parsePostData(params, input);
}

// Test a parameter that appears multiple times and has special characters
TEST(HTTPParsing, getrepeated) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla",
			"PARAM=one&PARAM=two&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F");

	EXPECT_EQ(params.get("param", ""), "äöüß");
}

// Test a parameter that has no value.
TEST(HTTPParsing, getnovalue) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla",
			"flag1&flag2&flag3&value1=3&value2=4");

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
			"application/x-www-form-urlencoded",
			"flag1&param=one&PARAM=two&flag2&pArAm=%C3%A4%C3%B6%C3%BC%C3%9F");

	EXPECT_EQ(params.get("flag1", "-"), "");
	EXPECT_EQ(params.get("flag2", "-"), "");
	EXPECT_EQ(params.get("param", ""), "äöüß");
}

// Test weird query string formats
TEST(HTTPParsing, testquerystringspecialchars) {
	HTTPService::Params params;
	parseCGIEnvironment(params, "GET", "/cgi-bin/bla",
			"p1&p2=1=2=%C3%A4%C3%B6%C3%BC%C3%9F&p3=&p4&&p5&?????&&&====&=&???&&p6==?");

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

// This is a multipart message that with content-disposition NOT set as "form-data". Thus, no parameter is parsed here and the body is ignored as always.
const std::string multipart_message =
					"\r\n"
					"This is a message with multiple parts in MIME format.\r\n"
					"--frontier\r\n"
					"Content-Type: text/plain\r\n"
					"\r\n"
					"This is the body of the message.\r\n"
					"--frontier\r\n"
					"Content-Type: application/octet-stream\r\n"
					"Content-Transfer-Encoding: base64\r\n"
					"\r\n"
					"PGh0bWw+CiAgPGhlYWQ+CiAgPC9oZWFkPgogIDxib2R5PgogICAgPHA+VGhpcyBpcyB0aGUg\r\n"
					"Ym9keSBvZiB0aGUgbWVzc2FnZS48L3A+CiAgPC9ib2R5Pgo8L2h0bWw+Cg==\r\n"
					"--frontier--"
					"--frontier--\r\n";

// This is a multipart message that with content-disposition set as "form-data". A 'name' key is provided.
const std::string multipart_message2 =
		"Content-Length: 554\r\n" // Since it may provoke overflow attacks, content-length is ignored.
		"\r\n"
		"----myboundary\r\n"
		"Content-Disposition: form-data; name=\"text\"\r\n"
		"\r\n"
		"text default\r\n"
		"----myboundary\r\n"
		"Content-Disposition: form-data; name=\"file1\"; filename=\"a.txt\"\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"Content of a.txt.\r\n"
		"\r\n"
		"----myboundary\r\n"
		"Content-Disposition: form-data; name=\"file2\"; filename=\"a.html\"\r\n"
		"Content-Type: text/html\r\n"
		"----myboundary--"
		;

// This is a multipart message that with content-disposition set as "form-data". A 'name' key is NOT provided (which is not allowed).
const std::string multipart_message3 =
		"--xyz\r\n"
		"Content-Disposition: form-data;\r\n"
		"xyz content\r\n"
		"--xyz--\r\n"
		;

// This is a multipart message with a corrupt boundary
const std::string multipart_message4 =
		"--xyz\r\n"
		"Content-Disposition: form-data;\r\n"
		"xyz content\r\n"
		;


TEST(HTTPParsing, multipart) {
	// Since frontier is not a form-data content disposition, this test should read nothing (but succeed).
	HTTPService::Params params;
	parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/mixed; boundary=frontier", multipart_message);
	EXPECT_TRUE(params.empty());
}

TEST(HTTPParsing, multipart_escaped_boundary) {
	// This test should read the params "file1" and "file2".
	HTTPService::Params params;
	parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/form-data; boundary=--myboundary                ", multipart_message2);

	EXPECT_TRUE(params.hasParam("file1"));
	EXPECT_TRUE(params.hasParam("file2"));
}

TEST(HTTPParsing, multipart_unnamed_formdata) {
	// This test should fail because the message contains form-data without a valid name.
	// Expected: Throw ArgumentException "
	HTTPService::Params params;
	EXPECT_THROW(parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/form-data; boundary=xyz", multipart_message3),
			ArgumentException);
}

TEST(HTTPParsing, multipart_malformed_boundary) {
	// This test should fail because the message is malformed (missing the closing tag of the boundary).
	// Expected: Throw runtime_error "Unexpected end of stream".
	HTTPService::Params params;
	EXPECT_THROW(parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/form-data; boundary=xyz", multipart_message4),
			std::runtime_error);

}

TEST(HTTPParsing, multipart_missing_boundary) {
	// This test should fail because the message is missing the specified boundary label.
	// Expected: Throw runtime_error "Unexpected end of stream".
	HTTPService::Params params;
	EXPECT_THROW(parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/form-data; boundary=xyz", ""),
			std::runtime_error);
}

TEST(HTTPParsing, multipart_unspecified_boundary) {
	// This test should succeed. No parameters should be read, because no boundary has been specified.
	HTTPService::Params params;
	parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","multipart/mixed", multipart_message);
	EXPECT_TRUE(params.empty());
}

TEST(HTTPParsing, parse_illegal_content_type) {
	// This test should fail due to the unknown content-type.
	// Expected: Throw ArgumentException "Unknown content type in POST request".
	HTTPService::Params params;
	EXPECT_THROW(parseCGIEnvironment(params, "POST", "/cgi-bin/bla", "","you-dont/know-me", multipart_message),
			ArgumentException);

}

TEST(HTTPParsing, case_insensitive_request_method_post) {
	// The test is exactly the same as the multipart_escaped_boundary test, the only difference being the character case in the REQUEST_METHOD parameter.
	// This should NOT fail as per design, but shouldn't parse any parameters as well.
	HTTPService::Params params;
	parseCGIEnvironment(params, "PoST", "/cgi-bin/bla", "","multipart/form-data; boundary=--myboundary", multipart_message2);
	EXPECT_TRUE(params.empty());

}

TEST(HTTPParsing, case_insensitive_request_method_get) {
	// The same test using GET. This time get parameters should exist.
	HTTPService::Params params;
	parseCGIEnvironment(params, "PoST", "/cgi-bin/bla", "a=1&b=2&c=3","multipart/form-data; boundary=--myboundary", multipart_message2);
	EXPECT_EQ(3, params.size());
	EXPECT_EQ(1, params.getInt("a"));
	EXPECT_EQ(2, params.getInt("b"));
	EXPECT_EQ(3, params.getInt("c"));
}


