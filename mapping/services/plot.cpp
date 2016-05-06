
#include "services/plot.h"
#include "datatypes/plot.h"
#include "operators/operator.h"

void PlotService::run(const Params& params, HTTPResponseStream& result, std::ostream &error) {
	std::string query = params.get("query", "");
	if(query == "")
		throw ArgumentException("PlotService: no query specified");

	// always query plots as Lat/Lon
	SpatialReference sref(epsg_t::EPSG_LATLON);

	TemporalReference tref = parseTime(params);

	auto graph = GenericOperator::fromJSON(query);

	QueryProfiler profiler;

	QueryRectangle rect(
		sref,
		tref,
		QueryResolution::none()
	);

	auto plot = graph->getCachedPlot(rect, profiler);

	result.sendContentType("application/json");
	result.finishHeaders();

	result << plot->toJSON();
}
