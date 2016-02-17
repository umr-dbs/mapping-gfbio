
#include "util/exceptions.h"

#include <istream>
#include <vector>


class CSVParser {
	public:
		CSVParser(std::istream &in, char field_separator = ',');
		~CSVParser();

		std::vector<std::string> readHeaders();
		std::vector<std::string> readTuple();

		class parse_error : public std::runtime_error {
			using std::runtime_error::runtime_error;
		};

	private:
		std::vector<std::string> parseLine();

		char field_separator;
		int field_count;
		int state; // this should be an enum, but it isn't. See comment in .cpp.
		size_t line_number;
		std::istream &in;
};



