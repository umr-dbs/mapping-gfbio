#include "operators/operator.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/csvparser.h"

#include <string>
#include <fstream>
#include <sstream>
#include <streambuf>
#include <algorithm>
#include <json/json.h>
#include <sys/stat.h>
#include "datatypes/pointcollection.h"

class CSVPointSource : public GenericOperator {
	public:
		CSVPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~CSVPointSource();

		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::string filename;
};


CSVPointSource::CSVPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	filename = params.get("filename", "").asString();
}

CSVPointSource::~CSVPointSource() {
}
REGISTER_OPERATOR(CSVPointSource, "csvpointsource");

void CSVPointSource::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"filename\":\"" << filename << "\"";
}

static bool endsWith(const std::string &str, const std::string &suffix) {
	if (str.length() < suffix.length())
		return false;

	return (str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0);
}

static uint64_t getFilesize(const char *filename) {
    struct stat st;
    if (stat(filename, &st) == 0)
        return st.st_size;
    return -1;
}

static double parseDate(const std::string &str) {
	std::tm tm;
	// 13-Jul-2010  17:35
	time_t t = 0;
	if (strptime(str.c_str(), "%d-%B-%Y  %H:%M", &tm)) {
		t = mktime(&tm);
	}
	return (double) t;
}

std::unique_ptr<PointCollection> CSVPointSource::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points_out = std::make_unique<PointCollection>(rect);

	auto filesize = getFilesize(filename.c_str());
	if (filesize <= 0)
		throw OperatorException("CSVPointSource: getFilesize() failed, unable to estimate I/O costs");
	profiler.addIOCost(filesize);


	std::ifstream data(filename);

	char separator = ',';
	if (endsWith(filename, ".ttx"))
		separator = '\t';

	//header
	CSVParser parser(data, separator, '\n');
	auto headers = parser.readHeaders();

	// Try to find the headers with geo-coordinates
	std::vector<bool> is_numeric(headers.size(), false);

	size_t no_pos = std::numeric_limits<size_t>::max();
	size_t pos_x = no_pos, pos_y = no_pos, pos_t_start = no_pos, pos_t_end = no_pos;
	for (size_t i=0; i < headers.size(); i++) {
		std::string lc = headers[i];
		std::transform(lc.begin(), lc.end(), lc.begin(), ::tolower);
		//fprintf(stderr, "column %d: '%s' -> '%s'\n", (int) i, headers[i].c_str(), lc.c_str());
		if (lc == "x" || lc == "lon" || lc == "longitude")
			pos_x = i;
		if (lc == "y" || lc == "lat" || lc == "latitude")
			pos_y = i;
		if (lc == "time" || lc == "time_start" || lc == "date" || lc == "datet")
			pos_t_start = i;
		if (lc == "time_end")
			pos_t_end = i;
		if (lc == "plz" || lc == "value")
			is_numeric[i] = true;
	}

	if (pos_x == no_pos || pos_y == no_pos)
		throw OperatorException("No georeferenced columns found in CSV. Name them \"x\" and \"y\", \"lat\" and \"lon\" or \"latitude\" and \"longitude\"");

	//TODO: distinguish between double and string properties
	for (size_t i=0; i < headers.size(); i++) {
		if (i == pos_x || i == pos_y || i == pos_t_start || i == pos_t_end)
			continue;
		if (is_numeric[i])
			points_out->local_md_value.addEmptyVector(headers[i]);
		else
			points_out->local_md_string.addEmptyVector(headers[i]);
	}

	auto minx = rect.minx();
	auto maxx = rect.maxx();
	auto miny = rect.miny();
	auto maxy = rect.maxy();

	while (true) {
		auto tuple = parser.readTuple();
		if (tuple.size() == 0)
			break;

		// Workaround for safecast data: ignore entries without coordinates
		if (tuple[pos_x] == "" || tuple[pos_y] == "")
			continue;

		double x, y;
		try {
			x = std::stod(tuple[pos_x]);
			y = std::stod(tuple[pos_y]);
		}
		catch (const std::exception &e) {
			throw OperatorException("Coordinate value in CSV is not a number");
		}
		if (x < minx || x > maxx || y < miny || y > maxy)
			continue;

		size_t idx = points_out->addSinglePointFeature(Coordinate(x, y));

		for (size_t i=0; i < tuple.size(); i++) {
			if (i == pos_x || i == pos_y || i == pos_t_start || i == pos_t_end)
				continue;
			if (is_numeric[i])
				points_out->local_md_value.set(idx, headers[i], std::strtod(tuple[i].c_str(), nullptr));
			else
				points_out->local_md_string.set(idx, headers[i], tuple[i]);
		}
		if (pos_t_start != no_pos) {
			auto t_start = parseDate(tuple[pos_t_start]);
			auto t_end = t_start + 1;
			if (pos_t_end != no_pos)
				t_end = parseDate(tuple[pos_t_end]);
			points_out->time_start.push_back(t_start);
			points_out->time_end.push_back(t_end);
		}
	}

	return points_out;
}
