
#include <iostream>
#include <vector>

class CSVParser {
	public:
		CSVParser(std::istream &in, char field_separator = ',', char line_separator = '\n');
		~CSVParser();

		std::vector<std::string> readHeaders();
		std::vector<std::string> readTuple();

	private:
		std::string readNonemptyLine();
		std::vector<std::string> parseLine();

		enum class State {
			FIELD_START,
			FIELD_INSIDE_QUOTED,
			FIELD_INSIDE_NONQUOTED,
			BEFORE_SEPARATOR
		};
		std::vector<std::string> header_names;
		std::vector<int> header_types;

		char field_separator;
		char line_separator;
		std::istream &in;
};



