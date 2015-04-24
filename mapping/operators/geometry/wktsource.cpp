#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include <json/json.h>

/**
 * Operator that takes Well-Known-Text as parameter
 */
class WKTPointSource : public GenericOperator {
	public:
		WKTPointSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			wkt = params.get("wkt", "").asString();
		}

		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			return WKBUtil::readPointCollection(wkt);
		}

		virtual ~WKTPointSource(){};

	private:
		std::string wkt;
};
REGISTER_OPERATOR(WKTPointSource, "wktpointsource");

class WKTLineSource : public GenericOperator {
	public:
	WKTLineSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			wkt = params.get("wkt", "").asString();
		}

		virtual std::unique_ptr<LineCollection> getLineCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			return WKBUtil::readLineCollection(wkt);
		}

		virtual ~WKTLineSource(){};

	private:
		std::string wkt;
};
REGISTER_OPERATOR(WKTLineSource, "wktlinesource");

class WKTPolygonSource : public GenericOperator {
	public:
		WKTPolygonSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
			assumeSources(0);
			wkt = params.get("wkt", "").asString();
		}

		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler){
			return WKBUtil::readPolygonCollection(wkt);
		}

		virtual ~WKTPolygonSource(){};

	private:
		std::string wkt;
};
REGISTER_OPERATOR(WKTPolygonSource, "wktpolygonsource");


