#include "raster/pointcollection.h"
#include "raster/geometry.h"

#include "operators/operator.h"
#include "util/make_unique.h"
#include "util/csvparser.h"

#include <string>
#include <fstream>
#include <sstream>
#include <streambuf>
#include <algorithm>
#include <json/json.h>


class CSVPointSource : public GenericOperator {
	public:
		CSVPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~CSVPointSource();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
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

static bool endsWith(const std::string &str, const std::string &suffix) {
	if (str.length() < suffix.length())
		return false;

	return (str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0);
}

std::unique_ptr<PointCollection> CSVPointSource::getPoints(const QueryRectangle &rect) {
	auto points_out = std::make_unique<PointCollection>(EPSG_LATLON);

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
	size_t pos_x = no_pos, pos_y = no_pos, pos_t = no_pos;
	for (size_t i=0; i < headers.size(); i++) {
		std::string lc = headers[i];
		std::transform(lc.begin(), lc.end(), lc.begin(), ::tolower);
		//fprintf(stderr, "column %d: '%s' -> '%s'\n", (int) i, headers[i].c_str(), lc.c_str());
		if (lc == "x" || lc == "lon" || lc == "longitude")
			pos_x = i;
		if (lc == "y" || lc == "lat" || lc == "latitude")
			pos_y = i;
		if (lc == "time" || lc == "date" || lc == "datet")
			pos_t = i;
		if (lc == "plz")
			is_numeric[i] = true;
	}

	if (pos_x == no_pos || pos_y == no_pos)
		throw OperatorException("No georeferenced columns found in CSV. Name them \"x\" and \"y\", \"lat\" and \"lon\" or \"latitude\" and \"longitude\"");

	if (pos_t != no_pos) {
		points_out->has_time = true;
	}

	//TODO: distinguish between double and string properties
	for (size_t i=0; i < headers.size(); i++) {
		if (i == pos_x || i == pos_y || i == pos_t)
			continue;
		if (is_numeric[i])
			points_out->local_md_value.addVector(headers[i]);
		else
			points_out->local_md_string.addVector(headers[i]);
	}

	auto minx = rect.minx();
	auto maxx = rect.maxx();
	auto miny = rect.miny();
	auto maxy = rect.maxy();

	while (true) {
		auto tuple = parser.readTuple();
		if (tuple.size() == 0)
			break;

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

		size_t idx = points_out->addPoint(x, y);

		for (size_t i=0; i < tuple.size(); i++) {
			if (i == pos_x || i == pos_y || i == pos_t)
				continue;
			if (is_numeric[i])
				points_out->local_md_value.set(idx, headers[i], std::strtod(tuple[i].c_str(), nullptr));
			else
				points_out->local_md_string.set(idx, headers[i], tuple[i]);
		}
		if (pos_t != no_pos) {
			const auto &str = tuple[pos_t];
			std::tm tm;
			// 13-Jul-2010  17:35
			time_t t = 0;
			if (strptime(str.c_str(), "%d-%B-%Y  %H:%M", &tm)) {
				t = mktime(&tm);
			}
			points_out->timestamps.push_back(t);
		}
	}
	//fprintf(stderr, data.str().c_str());
	return points_out;
}
