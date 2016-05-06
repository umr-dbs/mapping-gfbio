#ifndef SERVICES_PLOT_H
#define SERVICES_PLOT_H

#include "services/ogcservice.h"

/*
 * This class serves results of plot queries. Although it does not follow any OGC standard we make use of common
 * functionality and inherit from the OGCService class
 *
 * Query pattern: mapping_url/?service=plot&query={QUERY_STRING}&time={ISO_TIME}
 *
 * Plot queries are always queried in Lat/Lon CRS
 */
class PlotService : public OGCService {
public:
	PlotService() = default;
	virtual ~PlotService() = default;

	virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error);
};

REGISTER_HTTP_SERVICE(PlotService, "plot");

#endif
