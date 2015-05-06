#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
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
		}

		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			return WKBUtil::readPointCollection(wkt);
		}

		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler){
				return WKBUtil::readLineCollection(wkt);
			}

		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			return WKBUtil::readPolygonCollection(wkt);
		}

		void writeSemanticParameters(std::ostringstream& stream) {
			stream << "\"type\":\"" << type << "\","
					<< "\"wkt\":\"" << wkt << "\"";
		}

		virtual ~WKTSource(){};

	private:
		std::string wkt;
		std::string type;
};
REGISTER_OPERATOR(WKTSource, "wktsource");
