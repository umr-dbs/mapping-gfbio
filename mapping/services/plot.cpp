
#include "datatypes/plot.h"
#include "operators/operator.h"

#include "services/ogcservice.h"

/*
 * This class serves results of plot queries. Although it does not follow any OGC standard we make use of common
 * functionality and inherit from the OGCService class
 *
 * Query pattern: mapping_url/?service=plot&query={QUERY_STRING}&time={ISO_TIME}&bbox={x1,y1,x2,y2}&crs={EPSG:epsg}
 * For plots containing at least one raster source, additionally parameters width and height have to be specified
 */
class PlotService : public OGCService {
public:
	PlotService() = default;
	virtual ~PlotService() = default;

	virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error);
};

REGISTER_HTTP_SERVICE(PlotService, "plot");

void PlotService::run(const Params& params, HTTPResponseStream& result, std::ostream &error) {
	std::string query = params.get("query", "");
	if(query == "")
		throw ArgumentException("PlotService: no query specified");

	if(!params.hasParam("crs"))
		throw ArgumentException("PlotService: crs not specified");

	epsg_t queryEpsg = parseEPSG(params, "crs");

	SpatialReference sref(queryEpsg);
	if(params.hasParam("bbox")) {
		sref = parseBBOX(params.get("bbox"), queryEpsg);
	}

	TemporalReference tref = parseTime(params);

	auto graph = GenericOperator::fromJSON(query);

	QueryProfiler profiler;

	QueryResolution qres = QueryResolution::none();

	if(params.hasParam("width") && params.hasParam("height")) {
		qres = QueryResolution::pixels(params.getInt("width"), params.getInt("height"));
	}
	QueryRectangle rect(
		sref,
		tref,
		qres
	);

	auto plot = graph->getCachedPlot(rect, profiler);

	result.sendContentType("application/json");
	result.finishHeaders();

	result << plot->toJSON();
}
