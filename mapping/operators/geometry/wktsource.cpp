#include "operators/operator.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/simplefeaturecollections/wkbutil.h"
#include <json/json.h>

/**
 * Operator that takes Well-Known-Text as parameter
 */
class WKTPolygonSource : public GenericOperator {
	public:
		WKTPolygonSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params);

		virtual std::unique_ptr<PolygonCollection> getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler);

		virtual ~WKTPolygonSource(){};

	private:
		std::string wkt;
};


WKTPolygonSource::WKTPolygonSource(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	wkt = params.get("wkt", "").asString();
}


std::unique_ptr<PolygonCollection> WKTPolygonSource::getPolygonCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	return WKBUtil::readPolygonCollection(wkt);
}
REGISTER_OPERATOR(WKTPolygonSource, "wktpolygonsource");
