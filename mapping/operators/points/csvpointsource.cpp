#include "operators/operator.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"

#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/csvparser.h"
#include "util/enumconverter.h"
#include "util/timeparser.h"

#include <string>
#include <fstream>
#include <sstream>
#include <streambuf>
#include <algorithm>
#include <functional>
#include <json/json.h>
#include <sys/stat.h>



/*
 * Define a few enums (including string representations) for parameter parsing
 */
enum class FileType {
	CSV,
	TTX
};

enum class GeometrySpecification {
	XY,
	WKT
	// ShapeFile? Others?
};

const std::vector< std::pair<GeometrySpecification, std::string> > GeometrySpecificationMap {
	std::make_pair(GeometrySpecification::XY, "xy"),
	std::make_pair(GeometrySpecification::WKT, "wkt")
};

EnumConverter<GeometrySpecification> GeometrySpecificationConverter(GeometrySpecificationMap);

enum class TimeSpecification {
	NONE,
	START,
	START_END,
	START_DURATION
};

const std::vector< std::pair<TimeSpecification, std::string> > TimeSpecificationMap = {
	std::make_pair(TimeSpecification::NONE, "none"),
	std::make_pair(TimeSpecification::START, "start"),
	std::make_pair(TimeSpecification::START_END, "start+end"),
	std::make_pair(TimeSpecification::START_DURATION, "start+duration")
};

EnumConverter<TimeSpecification> TimeSpecificationConverter(TimeSpecificationMap);

enum class ErrorHandling {
	ABORT,
	SKIP,
	KEEP
};

const std::vector< std::pair<ErrorHandling, std::string> > ErrorHandlingMap {
	std::make_pair(ErrorHandling::ABORT, "abort"),
	std::make_pair(ErrorHandling::SKIP, "skip"),
	std::make_pair(ErrorHandling::KEEP, "keep")
};


EnumConverter<ErrorHandling>ErrorHandlingConverter(ErrorHandlingMap);

/*
 * Now define the operator
 */
class CSVPointSource : public GenericOperator {
	public:
		CSVPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~CSVPointSource();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		void readAnyCollection(SimpleFeatureCollection *collection, const QueryRectangle &rect, QueryProfiler &profiler,
			std::function<bool(const std::string &,const std::string &)> addFeature);

		std::string filename;
		FileType filetype;
		GeometrySpecification geometry_specification;
		TimeSpecification time_specification;
		double time_duration;
		std::string column_x;
		std::string column_y;
		std::string column_time1;
		std::string column_time2;
		std::unique_ptr<TimeParser> time1Parser;
		std::unique_ptr<TimeParser> time2Parser;
		std::vector<std::string> columns_numeric;
		std::vector<std::string> columns_textual;
		char field_separator;
		ErrorHandling errorHandling;
};


static bool endsWith(const std::string &str, const std::string &suffix) {
	if (str.length() < suffix.length())
		return false;

	return (str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0);
}


CSVPointSource::CSVPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);

	filename = params.get("filename", "").asString();
	if (endsWith(filename, ".ttx"))
		filetype = FileType::TTX;
	else
		filetype = FileType::CSV;

	std::string default_separator = (filetype == FileType::TTX ? "\t" : ",");
	auto configured_separator = params.get("separator", default_separator).asString();
	if (configured_separator.length() != 1)
		throw ArgumentException("CSVPointSource: Configured separator is not a single character");
	field_separator = configured_separator[0];

	geometry_specification = GeometrySpecificationConverter.from_json(params, "geometry");

	auto columns = params.get("columns", Json::Value(Json::ValueType::objectValue));
	column_x = columns.get("x", "x").asString();
	column_y = columns.get("y", "y").asString();

	time_specification = TimeSpecificationConverter.from_json(params, "time");
	time_duration = 0.0;
	if (time_specification == TimeSpecification::START)
		time_duration = params.get("duration", 1.0).asDouble();

	if(time_specification != TimeSpecification::NONE){
		column_time1 = columns.get("time1", "time1").asString();

		const Json::Value& time1Format = params.get("time1_format", Json::Value::null);
		time1Parser = TimeParser::createFromJson(time1Format);
	}

	if(time_specification == TimeSpecification::START_DURATION || time_specification == TimeSpecification::START_END){
		//TODO: check that time2 can be used as interval (e.g. format::seconds)
		column_time2 = columns.get("time2", "time2").asString();

		const Json::Value& time2Format = params.get("time2_format", Json::Value::null);
		time2Parser = TimeParser::createFromJson(time2Format);
	}


	auto textual = columns.get("textual", Json::Value(Json::ValueType::arrayValue));
    for (auto &name : textual)
    	columns_textual.push_back(name.asString());
    std::sort(columns_textual.begin(), columns_textual.end());

    auto numeric = columns.get("numeric", Json::Value(Json::ValueType::arrayValue));
    for (auto &name : numeric)
    	columns_numeric.push_back(name.asString());
    std::sort(columns_numeric.begin(), columns_numeric.end());

    // TODO: make sure no column names are reused multiple times?

    try {
    	errorHandling = ErrorHandlingConverter.from_json(params, "on_error");
    } catch (const ArgumentException& e){
    	errorHandling = ErrorHandling::ABORT;
    }
}


CSVPointSource::~CSVPointSource() {
}
REGISTER_OPERATOR(CSVPointSource, "csvpointsource");

void CSVPointSource::writeSemanticParameters(std::ostringstream& stream) {
	Json::Value params(Json::ValueType::objectValue);

	params["filename"] = filename;
	params["on_error"] = ErrorHandlingConverter.to_string(errorHandling);
	params["separator"] = std::string(1, field_separator);

	params["geometry"] = GeometrySpecificationConverter.to_string(geometry_specification);
	params["time"] = TimeSpecificationConverter.to_string(time_specification);
	if (time_specification == TimeSpecification::START_DURATION)
		params["duration"] = time_duration;

	Json::Value columns(Json::ValueType::objectValue);
	columns["x"] = column_x;
	if (geometry_specification != GeometrySpecification::WKT)
		columns["y"] = column_y;
	if (time_specification != TimeSpecification::NONE) {
		columns["time1"] = column_time1;
		params["time1_format"] = time1Parser->toJsonObject();
		if (time_specification != TimeSpecification::START) {
			columns["time2"] = column_time2;
			params["time2_format"] = time2Parser->toJsonObject();
		}
	}

	Json::Value textual(Json::ValueType::arrayValue);
	for (const auto &c : columns_textual)
		textual.append(c);
	Json::Value numeric(Json::ValueType::arrayValue);
	for (const auto &c : columns_numeric)
		numeric.append(c);

	columns["textual"] = textual;
	columns["numeric"] = numeric;
	params["columns"] = columns;

	Json::FastWriter writer;
	stream << writer.write(params);
}

#ifndef MAPPING_OPERATOR_STUBS
static uint64_t getFilesize(const char *filename) {
    struct stat st;
    if (stat(filename, &st) == 0)
        return st.st_size;
    return -1;
}

void CSVPointSource::readAnyCollection(SimpleFeatureCollection *collection, const QueryRectangle &rect, QueryProfiler &profiler,
		std::function<bool(const std::string &,const std::string &)> addFeature) {
	auto filesize = getFilesize(filename.c_str());
	if (filesize <= 0)
		throw OperatorException("CSVPointSource: getFilesize() failed, unable to estimate I/O costs");
	profiler.addIOCost(filesize);


	std::ifstream data(filename);

	//header
	CSVParser parser(data, field_separator, '\n');
	auto headers = parser.readHeaders();

	// Try to match up all headers
	size_t no_pos = std::numeric_limits<size_t>::max();
	size_t pos_x = no_pos, pos_y = no_pos, pos_time1 = no_pos, pos_time2 = no_pos;
	std::vector<size_t> pos_numeric(columns_numeric.size(), no_pos);
	std::vector<size_t> pos_textual(columns_textual.size(), no_pos);

	for (size_t i=0; i < headers.size(); i++) {
		const std::string &header = headers[i];
		//fprintf(stderr, "column %d: '%s' -> '%s'\n", (int) i, headers[i].c_str(), lc.c_str());
		if (header == column_x)
			pos_x = i;
		else if (header == column_y)
			pos_y = i;
		else if (header == column_time1)
			pos_time1 = i;
		else if (header == column_time2)
			pos_time2 = i;
		else {
			bool found=false;
			for (size_t k=0;k<columns_numeric.size();k++) {
				if (header == columns_numeric[k]) {
					found = true;
					pos_numeric[k] = i;
					break;
				}
			}
			if (found)
				continue;
			for (size_t k=0;k<columns_textual.size();k++) {
				if (header == columns_textual[k]) {
					pos_textual[k] = i;
					break;
				}
			}
		}
	}

	if (pos_x == no_pos || (geometry_specification == GeometrySpecification::XY && pos_y == no_pos))
		throw OperatorException("CSVPointSource: the given columns containing the geometry could not be found.");

	if((time1Parser != nullptr && pos_time1 == no_pos) || (time2Parser != nullptr && pos_time2 == no_pos))
		throw OperatorException("CSVPointSource: the given column containing time information could not be found.");

	if((time1Parser != nullptr && time1Parser->getTimeType() != rect.timetype) || (time2Parser != nullptr && time2Parser->getTimeType() != rect.timetype))
		throw OperatorException("CSVPointSource: Invalid time specification for given query rectangle");

	for (size_t k=0;k<columns_numeric.size();k++) {
		if (pos_numeric[k] == no_pos)
			throw OperatorException(concat("CSVPointSource: numeric column \"", columns_numeric[k], "\" not found."));
		collection->feature_attributes.addNumericAttribute(columns_numeric[k], Unit::unknown()); // TODO: units
	}

	for (size_t k=0;k<columns_textual.size();k++) {
		if (pos_textual[k] == no_pos)
			throw OperatorException(concat("CSVPointSource: textual column \"", columns_textual[k], "\" not found."));
		collection->feature_attributes.addTextualAttribute(columns_textual[k], Unit::unknown()); // TODO: units
	}


	const std::string empty_string = "";

	size_t current_idx = 0;
	while (true) {
		auto tuple = parser.readTuple();
		if (tuple.size() == 0)
			break;

		// Step 1: extract the geometry
		// Note: faulty geometries lead to an error; empty geometries are simply skipped
		const std::string &x_str = (pos_x == no_pos ? empty_string : tuple[pos_x]);
		const std::string &y_str = (pos_y == no_pos ? empty_string : tuple[pos_y]);
		bool added = false;
		try {
			added = addFeature(x_str, y_str);
		}
		catch (const std::exception &e) {
			switch(errorHandling) {
				case ErrorHandling::ABORT:
					throw OperatorException(concat("Geometry in CSV could not be parsed: '", x_str, "', '", y_str, "'"));
				case ErrorHandling::SKIP:
					continue;
				case ErrorHandling::KEEP:
					//TODO: ???? Insert 0-Feature?
					continue;
			}

		}
		if (!added)
			continue;

		// Step 2: extract the time information
		if (time_specification != TimeSpecification::NONE) {
			double t1, t2;
			bool error = false;
			if (time_specification == TimeSpecification::START) {
				try {
					t1 = time1Parser->parse(tuple[pos_time1]);
					t2 = t1+time_duration;
				} catch (const TimeParseException& e){
					t1 = rect.beginning_of_time();
					t2 = rect.end_of_time();
					error = true;
				}
			}
			else if (time_specification == TimeSpecification::START_END) {
				try {
					t1 = time1Parser->parse(tuple[pos_time1]);
				} catch (const TimeParseException& e){
					t1 = rect.beginning_of_time();
					error = true;
				}
				try {
					t2 = time2Parser->parse(tuple[pos_time2]);
				} catch (const TimeParseException& e){
					t2 = rect.end_of_time();
					error = true;
				}
			}
			else if (time_specification == TimeSpecification::START_DURATION) {
				try {
					t1 = time1Parser->parse(tuple[pos_time1]);
					t2 = t1 + time2Parser->parse(tuple[pos_time2]);
				} catch (const TimeParseException& e){
					t1 = rect.beginning_of_time();
					t2 = rect.end_of_time();
					error = true;
				}
			}

			if(error) {
				switch(errorHandling) {
					case ErrorHandling::ABORT:
						throw OperatorException("CSVSource: could not parse time");
					case ErrorHandling::SKIP:
						collection->removeLastFeature();
						continue;
					case ErrorHandling::KEEP:
						break;
				}
			}
			collection->time_start.push_back(t1);
			collection->time_end.push_back(t2);
		}



		// Step 3: extract the attributes
		for (size_t k=0;k<columns_numeric.size();k++) {
			double value;
			try {
				value = std::stod(tuple[pos_numeric[k]].c_str());
			} catch (const std::exception& e) {
				switch(errorHandling) {
					case ErrorHandling::ABORT:
						throw OperatorException("CSVSource: error parsing double value from string");
					case ErrorHandling::SKIP:
						collection->removeLastFeature();
						continue;
					case ErrorHandling::KEEP:
						break;
				}
			}
			collection->feature_attributes.numeric(columns_numeric[k]).set(current_idx, value);
		}
		for (size_t k=0;k<columns_textual.size();k++) {
			collection->feature_attributes.textual(columns_textual[k]).set(current_idx, tuple[pos_textual[k]]);
		}

		// Step 4: increase the current idx, since our feature is finished
		current_idx++;
	}
}


std::unique_ptr<PointCollection> CSVPointSource::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto collection = make_unique<PointCollection>(rect);
	auto add_xy = [&](const std::string &x_str, const std::string &y_str) -> bool {
		// Workaround for safecast data: ignore entries without coordinates
		if (x_str == "" || y_str == "")
			return false;

		double x, y;
		x = std::stod(x_str);
		y = std::stod(y_str);

		collection->addSinglePointFeature(Coordinate(x, y));
		return true;
	};
	auto add_wkt = [&](const std::string &wkt, const std::string &) -> bool {
		WKBUtil::addFeatureToCollection(*collection, wkt);
		return true;
	};
	if (geometry_specification == GeometrySpecification::XY) {
		readAnyCollection(collection.get(), rect, profiler, add_xy);
	}
	else if (geometry_specification == GeometrySpecification::WKT) {
		readAnyCollection(collection.get(), rect, profiler, add_wkt);
	}
	else
		throw OperatorException("Unimplemented geometry_specification for Points");

	collection->filterBySpatioTemporalReferenceIntersectionInPlace(rect);
	return collection;
}

std::unique_ptr<LineCollection> CSVPointSource::getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto collection = make_unique<LineCollection>(rect);
	auto add_wkt = [&](const std::string &wkt, const std::string &) -> bool {
		WKBUtil::addFeatureToCollection(*collection, wkt);
		return true;
	};
	if (geometry_specification == GeometrySpecification::WKT) {
		readAnyCollection(collection.get(), rect, profiler, add_wkt);
	}
	else
		throw OperatorException("Unimplemented geometry_specification for Lines");

	collection->filterBySpatioTemporalReferenceIntersectionInPlace(rect);
	return collection;
}

std::unique_ptr<PolygonCollection> CSVPointSource::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto collection = make_unique<PolygonCollection>(rect);
	auto add_wkt = [&](const std::string &wkt, const std::string &) -> bool {
		WKBUtil::addFeatureToCollection(*collection, wkt);
		return true;
	};
	if (geometry_specification == GeometrySpecification::WKT) {
		readAnyCollection(collection.get(), rect, profiler, add_wkt);
	}
	else
		throw OperatorException("Unimplemented geometry_specification for Polygons");

	collection->filterBySpatioTemporalReferenceIntersectionInPlace(rect);
	return collection;
}


#endif
