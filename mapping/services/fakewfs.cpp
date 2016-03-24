
// TODO: remove this class, use wfs instead.

#include "services/ogcservice.h"
#include "operators/operator.h"

#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"


class FakeWFSService : public OGCService {
	public:
		FakeWFSService() = default;
		virtual ~FakeWFSService() = default;
		virtual void run(const Params& params, HTTPResponseStream& result, std::ostream &error);
};
REGISTER_HTTP_SERVICE(FakeWFSService, "FAKEWFS");


void FakeWFSService::run(const Params &params, HTTPResponseStream& result, std::ostream &error) {
	auto query_epsg = parseEPSG(params, "crs");
	auto timestamp = parseTimestamp(params);

	// PointCollection as GeoJSON
	if (params.hasParam("pointquery")) {
		auto graph = GenericOperator::fromJSON(params.get("pointquery"));

		QueryRectangle rect(
			SpatialReference::extent(query_epsg),
			TemporalReference(TIMETYPE_UNIX, timestamp),
			QueryResolution::none()
		);
		QueryProfiler profiler;
		auto points = graph->getCachedPointCollection(rect, profiler);

		auto format = params.get("format", "geojson");
		if (format == "csv") {
			outputSimpleFeatureCollectionCSV(result, points.get());
		}
		else if (format == "geojson") {
			outputSimpleFeatureCollectionGeoJSON(result, points.get(), true);
		}
		else if (format == "arff") {
			outputSimpleFeatureCollectionARFF(result, points.get());
		}

		return;
	}

	if (params.hasParam("linequery")) {
		auto graph = GenericOperator::fromJSON(params.get("linequery"));

		QueryRectangle rect(
			SpatialReference::extent(query_epsg),
			TemporalReference(TIMETYPE_UNIX, timestamp),
			QueryResolution::none()
		);

		QueryProfiler profiler;
		auto lines = graph->getCachedLineCollection(rect, profiler);

		auto format = params.get("format", "geojson");
		if (format == "csv") {
			outputSimpleFeatureCollectionCSV(result, lines.get());
		}
		else if (format == "geojson") {
			outputSimpleFeatureCollectionGeoJSON(result, lines.get(), true);
		}
		else if (format == "arff") {
			outputSimpleFeatureCollectionARFF(result, lines.get());
		}
		return;
	}

	if (params.hasParam("polygonquery")) {
		auto graph = GenericOperator::fromJSON(params.get("polygonquery"));

		QueryRectangle rect(
			SpatialReference::extent(query_epsg),
			TemporalReference(TIMETYPE_UNIX, timestamp),
			QueryResolution::none()
		);
		QueryProfiler profiler;
		auto polygons = graph->getCachedPolygonCollection(rect, profiler);

		auto format = params.get("format", "geojson");
		if (format == "csv") {
			outputSimpleFeatureCollectionCSV(result, polygons.get());
		}
		else if (format == "geojson") {
			outputSimpleFeatureCollectionGeoJSON(result, polygons.get(), true);
		}
		else if (format == "arff") {
			outputSimpleFeatureCollectionARFF(result, polygons.get());
		}
		return;
	}

	result.send500("FakeWFS: no query found");
}
