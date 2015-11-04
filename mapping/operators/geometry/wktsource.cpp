#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include "util/exceptions.h"
#include <json/json.h>

/**
 * Operator that takes Well-Known-Text as parameter
 */
class WKTSource : public GenericOperator {
	public:
	WKTSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			wkt = params.get("wkt", "").asString();
			type = params.get("type", "").asString();

			if(type != "points" && type != "lines" && type != "polygons")
				throw ArgumentException("Invalid type");
		}

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "points")
				throw ArgumentException("WKTSource does not contain points");
			return WKBUtil::readPointCollection(wkt);
		}

		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "lines")
				throw ArgumentException("WKTSource does not contain lines");
			return WKBUtil::readLineCollection(wkt);
		}

		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			if(type != "polygons")
				throw ArgumentException("WKTSource does not contain polygons");
			return WKBUtil::readPolygonCollection(wkt);
		}
#endif

		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "{\"type\":\"" << type << "\","
					<< "\"wkt\":\"" << wkt << "\"}";
		}

		virtual ~WKTSource(){};

	private:
		std::string wkt;
		std::string type;
};
REGISTER_OPERATOR(WKTSource, "wktsource");
