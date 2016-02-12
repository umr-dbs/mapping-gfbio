#include "util/csvparser.h"
#include "util/exceptions.h"

#include <gtest/gtest.h>
#include <string>
#include <sstream>

static void toCSV(std::stringstream& ss, const std::vector<std::vector<std::string>>& result, const std::string& delim, const std::string& endl) {
	for(auto fields : result ){
		ss << fields[0];
		for(size_t i = 1; i < fields.size(); ++i){
			ss << delim << fields[i];
		}
		ss << endl;
	}
}

struct CSVTest {
	const std::vector<std::vector<std::string>> input;
	const std::vector<std::vector<std::string>> result;
};

static const CSVTest simple = {
	{{"a", "b", "c"}, {"testa1", "testb1", "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}},
	{{"a", "b", "c"}, {"testa1", "testb1", "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}}
};


static const CSVTest quotes = {
	{{"a", "b", "c"}, {"\"testa1\"", "\"testb \"\"1\"\"\"", "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}},
	{{"a", "b", "c"}, {"testa1", "testb \"1\"", "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}}
};


static CSVTest lineBreaksInQuotes(const std::string& endl){
	CSVTest result = {
		{{"a", "b", "c"}, {"\"test" + endl + "a1\"", "\"testb" + endl + endl + "\"\"1\"\"" + endl + "\"", "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}},
		{{"a", "b", "c"}, {"test" + endl + "a1", "testb" + endl + endl + "\"1\"" + endl, "testc1"}, {"d!\u00A7\00FC %&/()", "", "f"}},
	};
	return result;
}

static CSVTest delimInQuotes(const std::string& delim){
	CSVTest result = {
		{{"a", "b", "c"}, {"\"test" + delim + "a1\"", "\"testb" + delim + delim + "\"\"1\"\"" + delim + "\"", "testc1"}, {"d", "e", "f"}},
		{{"a", "b", "c"}, {"test" + delim + "a1", "testb" + delim + delim + "\"1\"" + delim, "testc1"}, {"d", "e", "f"}}
	};
	return result;
}

static const std::vector<std::vector<std::string>> missingFields({{"a", "b", "c"}, {"d"}, {"e", "f", "g"}});

static const std::vector<std::vector<std::string>> tooManyFields({{"a", "b", "c"}, {"d", "e", "f", "g"}, {"h", "i", "j"}});

static void checkParseResult(CSVParser& parser, const std::vector<std::vector<std::string>> expected) {
	for(std::vector<std::string> fields : expected) {
		auto tuple = parser.readTuple();
		ASSERT_EQ(fields.size(), tuple.size());

		for(size_t i = 0; i < fields.size(); ++i) {
			ASSERT_EQ(fields[i], tuple[i]);
		}
	}
	// test if the file actually ended here
	auto tuple = parser.readTuple();
	ASSERT_EQ(0, tuple.size());
}


TEST(CSVParser, simpleComma) {
	std::string delim = ",";
	std::string endl = "\n";
	auto& test = simple;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, simpleSemicolon) {
	std::string delim = ";";
	std::string endl = "\n";
	auto& test = simple;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, simpleCommaCRLF) {
	std::string delim = ",";
	std::string endl = "\r\n";
	auto& test = simple;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, simpleSemicolonCRLF) {
	std::string delim = ";";
	std::string endl = "\r\n";
	auto& test = simple;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, DISABLED_simpleWrongDelim) {
	std::string delim = ";";
	std::string endl = "\n";
	auto& test = simple;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, ',');
	checkParseResult(parser, test.result);
}

TEST(CSVParser, quotes) {
	std::string delim = ",";
	std::string endl = "\n";
	auto& test = quotes;
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, lineBreaksLF) {
	std::string delim = ",";
	std::string endl = "\n";
	auto test = lineBreaksInQuotes(endl);
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);

}

TEST(CSVParser, lineBreaksCRLF) {
	std::string delim = ",";
	std::string endl = "\r\n";
	auto test = lineBreaksInQuotes(endl);
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, delimInQuotesComma) {
	std::string delim = ",";
	std::string endl = "\n";
	auto test = delimInQuotes(delim);
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, delimInQuotesSemicolon) {
	std::string delim = ";";
	std::string endl = "\n";
	auto test = delimInQuotes(delim);
	std::stringstream ss;
	toCSV(ss, test.input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	checkParseResult(parser, test.result);
}

TEST(CSVParser, missingFields) {
	std::string delim = ",";
	std::string endl = "\n";
	auto& input = missingFields;
	std::stringstream ss;
	toCSV(ss, input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	EXPECT_THROW(checkParseResult(parser, input), CSVParser::parse_error);
}

TEST(CSVParser, tooManyFieldsFields) {
	std::string delim = ",";
	std::string endl = "\n";
	auto& input = tooManyFields;
	std::stringstream ss;
	toCSV(ss, input, delim, endl);
	CSVParser parser(ss, delim.at(0));
	EXPECT_THROW(checkParseResult(parser, input), CSVParser::parse_error);
}

