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
#include <vector>

class RasterMetaDataToPoints: public GenericOperator {
	public:
		RasterMetaDataToPoints(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~RasterMetaDataToPoints();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);

	private:
		std::vector<std::string> names;
};

RasterMetaDataToPoints::RasterMetaDataToPoints(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
//	assumeSources(2);

	names.clear();
	auto arr = params["names"];
	if (!arr.isArray())
		throw OperatorException("raster_metadata_to_points: names parameter invalid");

	int len = (int) arr.size();
	names.reserve(len);
	for (int i=0;i<len;i++) {
		names.push_back( arr[i].asString() );
	}
}

RasterMetaDataToPoints::~RasterMetaDataToPoints() {
}
REGISTER_OPERATOR(RasterMetaDataToPoints, "raster_metadata_to_points");

template<typename T>
struct PointDataEnhancement {
	static void execute(Raster2D<T>* raster, PointCollection *points, const std::string &name) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		//T max = (T) raster->dd.max;
		//T min = (T) raster->dd.min;

		// add global metadata
		// TODO: resolve what to do on key-collision
		//points->setGlobalMDValue(name + "_max", max);
		//points->setGlobalMDValue(name + "_min", min);

		// init local metadata
		auto &md_vec = points->local_md_value.addVector(name);

		for (auto &point : points->collection) {
			size_t rasterCoordinateX = floor(raster->lcrs.WorldToPixelX(point.x));
			size_t rasterCoordinateY = floor(raster->lcrs.WorldToPixelY(point.y));

			double md = std::numeric_limits<double>::quiet_NaN();
			if (rasterCoordinateX >= 0 && rasterCoordinateY >= 0 &&	rasterCoordinateX < raster->lcrs.size[0] && rasterCoordinateY < raster->lcrs.size[1]) {
				T value = raster->get(rasterCoordinateX, rasterCoordinateY);
				if (!raster->dd.is_no_data(value))
					md = (double) value;
			}
			md_vec.push_back(md);
		}
	}
};

std::unique_ptr<PointCollection> RasterMetaDataToPoints::getPoints(const QueryRectangle &rect) {
	Profiler::Profiler p("RASTER_METADATA_TO_POINTS_OPERATOR");

	auto points = getPointsFromSource(0, rect);

	if (points->has_time) {
		// TODO: sort by time, iterate over all timestamps, fetch the correct raster, then add metadata
		throw OperatorException("raster_metadata_to_points: Cannot yet handle PointCollections with timestamps");
	}

	auto rasters = getRasterSourceCount();
	for (int r=0;r<rasters;r++) {
		auto raster = getRasterFromSource(r, rect);
		callUnaryOperatorFunc<PointDataEnhancement>(raster.get(), points.get(), names.at(r));
	}
	return points;
}
