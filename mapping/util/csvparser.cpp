
#include "raster/exceptions.h"
#include "util/csvparser.h"


CSVParser::CSVParser(std::istream &in, char field_separator, char line_separator)
	: in(in), field_separator(field_separator), line_separator(line_separator) {

}

CSVParser::~CSVParser() {

}


std::vector<std::string> CSVParser::readHeaders() {
	header_names = parseLine();
	return header_names;
}
std::vector<std::string> CSVParser::readTuple() {
	// todo: compare tuple count to headers
	// todo: statistics about quoted vs unquoted, string vs numerical
	return parseLine();
}

std::vector<std::string> CSVParser::parseLine() {
	std::string line = readNonemptyLine();

	if (line == "")
		return std::vector<std::string>();

	std::vector<std::string> fields;

	State state = State::FIELD_START;

	char escape = '\"';
	char quote = '\"';
	size_t field_start = 0;

	size_t linelen = line.length();
	for (size_t i = 0; i < linelen; i++) {
		char c = line[i];
		switch(state) {
			case State::FIELD_START:
				if (c == field_separator)
					throw OperatorException("CSV invalid: empty field");
				if (c == quote) {
					state = State::FIELD_INSIDE_QUOTED;
					field_start = i + 1;
				}
				else {
					state = State::FIELD_INSIDE_NONQUOTED;
					field_start = i;
				}
				break;
			case State::FIELD_INSIDE_QUOTED:
				if (c == line_separator)
					throw OperatorException("CSV invalid: quoted field not terminated by quote"); // TODO: is that correct, or do we keep the separator as a literal?
				if (c == quote) {
					fields.push_back(line.substr(field_start, i-field_start));
					state = State::BEFORE_SEPARATOR;
				}
				break;
			case State::FIELD_INSIDE_NONQUOTED:
				if (c == field_separator) {
					fields.push_back(line.substr(field_start, i-field_start));
					state = State::FIELD_START;
				}
				else if (c == quote)
					throw OperatorException("CSV invalid: quote inside unquoted field");
				break;
			case State::BEFORE_SEPARATOR:
				if (c == field_separator)
					state = State::FIELD_START;
				else
					throw OperatorException("CSV invalid: quote inside unquoted field");
				break;
		}
	}

	// end of line, see which state we're in
	switch(state) {
		case State::FIELD_START:
			// this is ok.
			break;
		case State::FIELD_INSIDE_QUOTED:
			throw OperatorException("CSV invalid: end of line inside quoted field");
		case State::FIELD_INSIDE_NONQUOTED:
			fields.push_back(line.substr(field_start, std::string::npos));
			break;
		case State::BEFORE_SEPARATOR:
			// this is ok, too.
			break;
	}

	return fields;
}


std::string CSVParser::readNonemptyLine() {
	std::string line;
	while (true) {
		if (!in)
			return std::string("");
		std::getline(in,line);
		while(line[ line.length() - 1] == '\r')
			line.erase(line.length()-1);
		if (line.length() > 0)
			return line;
	}
}
