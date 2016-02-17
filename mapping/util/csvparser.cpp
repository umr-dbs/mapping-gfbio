
#include "util/csvparser.h"


// CSVParser::state is of this enum. It's declared as 'int' in the header to keep this enum private.
enum State {
	LINE_START,
	FIELD_START,
	IN_QUOTED_FIELD,
	QUOTE_IN_QUOTED_FIELD,
	IN_UNQUOTED_FIELD,
	END_OF_FILE
};


CSVParser::CSVParser(std::istream &in, char field_separator)
	: field_separator(field_separator), field_count(-1), in(in), line_number(0) {
	state = LINE_START;
}

CSVParser::~CSVParser() {

}


std::vector<std::string> CSVParser::readHeaders() {
	return readTuple();
}
std::vector<std::string> CSVParser::readTuple() {
	// todo: statistics about quoted vs unquoted, string vs numerical?
	auto tuple = parseLine();
	++line_number;

	if (!tuple.empty()) {
		if (field_count < 0)
			field_count = tuple.size();
		else if (field_count != tuple.size())
			throw parse_error("CSV invalid: file contains lines with different field counts");
	}

	return tuple;
}

std::vector<std::string> CSVParser::parseLine() {

	std::string current_field;
	std::vector<std::string> current_tuple;

	if (state == END_OF_FILE)
		return current_tuple;
	if (state != LINE_START)
		throw MustNotHappenException("CSVParser::parseLine() started in a state != LINE_START");


	while (true) {
		auto c = in.get();

		bool is_eof = (c == std::char_traits<char>::eof());
		bool is_line_separator = (c == '\r' || c == '\n');
		bool is_field_separator = (c == field_separator);
		bool is_quote = (c == '"');
		bool is_other = !is_eof && !is_line_separator && !is_field_separator && !is_quote;

		if (state == LINE_START) {
			// We're at the beginning of the file or just encountered a line_separator.
			if (is_eof) {
				state = END_OF_FILE;
				return current_tuple;
			}
			else if (is_line_separator) {
				continue;
			}
			else if (is_field_separator) {
				current_tuple.emplace_back("");
				state = FIELD_START;
			}
			else if (is_quote) {
				current_field.clear();
				state = IN_QUOTED_FIELD;
			}
			else if (is_other) {
				current_field = c;
				state = IN_UNQUOTED_FIELD;
			}
		}
		else if (state == FIELD_START) {
			// We just encountered a field_separator
			if (is_eof || is_line_separator) {
				current_tuple.emplace_back("");
				state = is_eof ? END_OF_FILE : LINE_START;
				return current_tuple;
			}
			else if (is_field_separator) {
				current_tuple.emplace_back("");
			}
			else if (is_quote) {
				current_field.clear();
				state = IN_QUOTED_FIELD;
			}
			else if (is_other) {
				current_field = c;
				state = IN_UNQUOTED_FIELD;
			}
		}
		else if (state == IN_QUOTED_FIELD) {
			// We encountered a quote and are now assembling the field's contents into current_field
			if (is_eof) {
				throw parse_error(concat("CSV invalid: quoted field does not end with a quote on line ", line_number));
			}
			else if (is_quote) {
				state = QUOTE_IN_QUOTED_FIELD;
			}
			else if (is_line_separator || is_field_separator || is_other) {
				current_field += c;
			}
		}
		else if (state == QUOTE_IN_QUOTED_FIELD) {
			// While assembling a quoted field, we encountered a quote.
			// This may either end the field OR an escaped quote, depending on this next character.
			if (is_eof || is_line_separator) {
				current_tuple.emplace_back(current_field);
				state = is_eof ? END_OF_FILE : LINE_START;
				return current_tuple;
			}
			else if (is_field_separator) {
				current_tuple.emplace_back(current_field);
				state = FIELD_START;
			}
			else if (is_quote) {
				current_field += '"';
				state = IN_QUOTED_FIELD;
			}
			else if (is_other) {
				throw parse_error(concat("CSV invalid: quoted field was not followed by a separator on line ", line_number));
			}
		}
		else if (state == IN_UNQUOTED_FIELD) {
			// We encountered no quote and are now assembling the field's contents into current_field
			if (is_eof || is_line_separator) {
				current_tuple.emplace_back(current_field);
				state = is_eof ? END_OF_FILE : LINE_START;
				return current_tuple;
			}
			else if (is_field_separator) {
				current_tuple.emplace_back(current_field);
				state = FIELD_START;
			}
			else if (is_quote) {
				throw parse_error(concat("CSV invalid: Found a quote inside an unquoted field on line ", line_number));
			}
			else if (is_other) {
				current_field += c;
			}
		}
		else
			throw MustNotHappenException("CSVParser: reached an invalid state");
	}

	// this is dead code, but it prevents eclipse from complaining..
	return {};
}
