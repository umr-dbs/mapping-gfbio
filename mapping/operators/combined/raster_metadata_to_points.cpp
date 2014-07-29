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
private:
	std::string name;
public:
	RasterMetaDataToPoints(int sourcecount, GenericOperator *sources[], Json::Value &params);
	virtual ~RasterMetaDataToPoints();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};

RasterMetaDataToPoints::RasterMetaDataToPoints(int sourcecount,	GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(2);

	name = params.get("name", "raster").asString();
}

RasterMetaDataToPoints::~RasterMetaDataToPoints() {
}
REGISTER_OPERATOR(RasterMetaDataToPoints, "raster_metadata_to_points");

template<typename T>
struct PointDataEnhancement {
	static std::unique_ptr<PointCollection> execute(Raster2D<T>* raster, PointCollection* points, std::string name) {
		raster->setRepresentation(GenericRaster::Representation::CPU);

		T max = (T) raster->dd.max;
		T min = (T) raster->dd.min;

		std::unique_ptr<PointCollection> pointCollection = std::make_unique<PointCollection>(points->epsg);
		//
		//copy old global data
		//
		for (auto pair : *(points->getGlobalMDValueIterator())) {
			pointCollection->setGlobalMDValue(pair.first, pair.second);
		}
		for (auto pair : *(points->getGlobalMDStringIterator())) {
			pointCollection->setGlobalMDString(pair.first, pair.second);
		}

		//
		// add new meta data,
		//
		/*
		  auto range = RasterTypeInfo<T>::getRange(min, max);
		  pointCollection->setGlobalMDValue(name + "_range", range);
		*/
		// TODO: resolve what to do on key-collision
		pointCollection->setGlobalMDValue(name + "_max", max);
		pointCollection->setGlobalMDValue(name + "_min", min);
		pointCollection->setGlobalMDValue(name + "_no_data", raster->dd.no_data);
		pointCollection->setGlobalMDValue(name + "_has_no_data", raster->dd.has_no_data); // bool -> 0/1
		pointCollection->addLocalMDValue(name);

		auto localMDStringKeys = points->getLocalMDStringKeys();
		auto localMDValueKeys = points->getLocalMDValueKeys();

		for (Point &p : points->collection) {
			double x = p.x, y = p.y;

			int px = floor(raster->lcrs.WorldToPixelX(x));
			int py = floor(raster->lcrs.WorldToPixelY(y));

			if (px >= 0 && py >= 0 && (size_t) px < raster->lcrs.size[0] && (size_t) py < raster->lcrs.size[1]) {
				Point& point = pointCollection->addPoint(x, y);

				// copy old meta data
				for(auto key : localMDStringKeys) {
					pointCollection->setLocalMDString(point, key, points->getLocalMDString(p, key));
				}
				for (auto key : localMDValueKeys) {
					pointCollection->setLocalMDValue(point, key, points->getLocalMDValue(p, key));
				}

				// add new meta data
				// TODO: resolve what to do on key-collision
				T value = raster->get(px, py);
				pointCollection->setLocalMDValue(point, name, value);
			}
		}

		return pointCollection;
	}
};

std::unique_ptr<PointCollection> RasterMetaDataToPoints::getPoints(
		const QueryRectangle &rect) {
	auto points = sources[0]->getPoints(rect);
	auto raster = sources[1]->getRaster(rect);

	Profiler::Profiler p("RASTER_METADATA_TO_POINTS_OPERATOR");
	return callUnaryOperatorFunc<PointDataEnhancement>(raster.get(),
			points.get(), name);
}
