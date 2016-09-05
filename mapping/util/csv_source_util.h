#ifndef UTIL_CSV_SOURCE_UTIL_H
#define UTIL_CSV_SOURCE_UTIL_H

#include "util/exceptions.h"

#include "util/enumconverter.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "util/timeparser.h"

#include <istream>
#include <vector>
#include <json/json.h>


/**
 * Define a few enums (including string representations) for parameter parsing
 */

enum class GeometrySpecification {
	XY,
	WKT
	// ShapeFile? Others?
};

const std::vector< std::pair<GeometrySpecification, std::string> > GeometrySpecificationMap {
	std::make_pair(GeometrySpecification::XY, "xy"),
	std::make_pair(GeometrySpecification::WKT, "wkt")
};

static EnumConverter<GeometrySpecification> GeometrySpecificationConverter(GeometrySpecificationMap);

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

static EnumConverter<TimeSpecification> TimeSpecificationConverter(TimeSpecificationMap);

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


static EnumConverter<ErrorHandling>ErrorHandlingConverter(ErrorHandlingMap);


/**
 * This class encapsulates parsing a csv file and creating feature collections
 */
class CSVSourceUtil {
	public:
		CSVSourceUtil(Json::Value &params);
		CSVSourceUtil(GeometrySpecification geometry_specification,
				TimeSpecification time_specification, double time_duration,
				std::string column_x, std::string column_y,
				std::string column_time1, std::string column_time2,
				std::vector<std::string> columns_numeric,
				std::vector<std::string> columns_textual, char field_separator,
				ErrorHandling errorHandling);

		~CSVSourceUtil();

		Json::Value getParameters();

		std::unique_ptr<PointCollection> getPointCollection(std::istream &data, const QueryRectangle &rect);
		std::unique_ptr<LineCollection> getLineCollection(std::istream &data, const QueryRectangle &rect);
		std::unique_ptr<PolygonCollection> getPolygonCollection(std::istream &data, const QueryRectangle &rect);

		void readAnyCollection(SimpleFeatureCollection *collection, std::istream &data, const QueryRectangle &rect,
					std::function<bool(const std::string &,const std::string &)> addFeature);

		Json::Value params;

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

#endif
