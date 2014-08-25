#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/histogram.h"
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
	static std::unique_ptr<PointCollection> execute(Raster2D<T>* raster, PointCollection* pointsOld, std::string name) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		auto pointsNew = std::make_unique<PointCollection>(pointsOld->epsg);

		PointCollectionMetadataCopier metadataCopier(*pointsOld, *pointsNew);
		metadataCopier.copyGlobalMetadata();

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		// add global metadata
		// TODO: resolve what to do on key-collision
		pointsNew->setGlobalMDValue(name + "_max", max);
		pointsNew->setGlobalMDValue(name + "_min", min);
		pointsNew->setGlobalMDValue(name + "_no_data", raster->dd.no_data);
		pointsNew->setGlobalMDValue(name + "_has_no_data", raster->dd.has_no_data); // bool -> 0/1

		// init local metadata
		metadataCopier.initLocalMetadataFields();
		pointsNew->addLocalMDValue(name);

		for (Point &pointOld : pointsOld->collection) {
			int rasterCoordinateX = floor(raster->lcrs.WorldToPixelX(pointOld.x));
			int rasterCoordinateY = floor(raster->lcrs.WorldToPixelY(pointOld.y));

			if (rasterCoordinateX >= 0 && rasterCoordinateY >= 0 &&	(size_t) rasterCoordinateX < raster->lcrs.size[0] && (size_t) rasterCoordinateY < raster->lcrs.size[1]) {
				Point& pointNew = pointsNew->addPoint(pointOld.x, pointOld.y);

				metadataCopier.copyLocalMetadata(pointOld, pointNew);

				// add new meta data
				// TODO: resolve what to do on key-collision
				T value = raster->get(rasterCoordinateX, rasterCoordinateY);
				pointsNew->setLocalMDValue(pointNew, name, value);
			}
		}

		return pointsNew;
	}
};

std::unique_ptr<PointCollection> RasterMetaDataToPoints::getPoints(const QueryRectangle &rect) {
	auto points = getPointsFromSource(0, rect);
	auto raster = getRasterFromSource(0, rect);

	Profiler::Profiler p("RASTER_METADATA_TO_POINTS_OPERATOR");
	return callUnaryOperatorFunc<PointDataEnhancement>(raster.get(), points.get(), name);
}
