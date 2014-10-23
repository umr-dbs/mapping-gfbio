#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include <cmath>
#include <json/json.h>
#include <algorithm>

class RasterMetaDataToPoints: public GenericOperator {
	public:
		RasterMetaDataToPoints(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~RasterMetaDataToPoints();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);

	private:
		std::string name;
};

RasterMetaDataToPoints::RasterMetaDataToPoints(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);

	name = params.get("name", "raster").asString();
}

RasterMetaDataToPoints::~RasterMetaDataToPoints() {
}
REGISTER_OPERATOR(RasterMetaDataToPoints, "raster_metadata_to_points");

template<typename T>
struct PointDataEnhancement {
	static void execute(Raster2D<T>* raster, PointCollection *points, std::string name) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		// add global metadata
		// TODO: resolve what to do on key-collision
		points->setGlobalMDValue(name + "_max", max);
		points->setGlobalMDValue(name + "_min", min);
		points->setGlobalMDValue(name + "_no_data", raster->dd.no_data);
		points->setGlobalMDValue(name + "_has_no_data", raster->dd.has_no_data); // bool -> 0/1

		// init local metadata
		auto &md_vec = points->local_md_value.addVector(name);

		for (auto &point : points->collection) {
			size_t rasterCoordinateX = floor(raster->lcrs.WorldToPixelX(point.x));
			size_t rasterCoordinateY = floor(raster->lcrs.WorldToPixelY(point.y));

			double md = raster->dd.no_data;
			if (rasterCoordinateX >= 0 && rasterCoordinateY >= 0 &&	rasterCoordinateX < raster->lcrs.size[0] && rasterCoordinateY < raster->lcrs.size[1]) {
				md = (double) raster->get(rasterCoordinateX, rasterCoordinateY);
			}
			md_vec.push_back(md);
		}
	}
};

std::unique_ptr<PointCollection> RasterMetaDataToPoints::getPoints(const QueryRectangle &rect) {
	auto points = getPointsFromSource(0, rect);

	if (points->has_time) {
		// TODO: sort by time, iterate over all timestamps, fetch the correct raster, then add metadata
		throw OperatorException("raster_metadata_to_points: Cannot yet handle PointCollections with timestamps");
	}

	auto raster = getRasterFromSource(0, rect);

	Profiler::Profiler p("RASTER_METADATA_TO_POINTS_OPERATOR");
	callUnaryOperatorFunc<PointDataEnhancement>(raster.get(), points.get(), name);
	return points;
}
