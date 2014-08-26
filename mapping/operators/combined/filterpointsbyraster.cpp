
#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/pointcollection.h"
#include "operators/operator.h"

#include "util/make_unique.h"


class FilterPointsByRaster : public GenericOperator {
	public:
		FilterPointsByRaster(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~FilterPointsByRaster();

		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};




FilterPointsByRaster::FilterPointsByRaster(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(2);
}

FilterPointsByRaster::~FilterPointsByRaster() {
}
REGISTER_OPERATOR(FilterPointsByRaster, "filterpointsbyraster");


std::unique_ptr<PointCollection> FilterPointsByRaster::getPoints(const QueryRectangle &rect) {

	auto points = getPointsFromSource(0, rect);
	auto raster = getRasterFromSource(0, rect);
	raster->setRepresentation(GenericRaster::Representation::CPU);

	auto points_out = std::make_unique<PointCollection>(rect.epsg);

	double no_data = 0.0; // 0 is always considered "false"
	if (raster->dd.has_no_data)
		no_data = raster->dd.no_data;

	for (Point &p : points->collection) {
		double x = p.x, y = p.y;

		int px = floor(raster->lcrs.WorldToPixelX(x));
		int py = floor(raster->lcrs.WorldToPixelY(y));

		if (px >= 0 && py >= 0 && (size_t) px < raster->lcrs.size[0] && (size_t) py < raster->lcrs.size[1]) {
			double value = raster->getAsDouble(px, py);

			if (value != 0.0 && value != no_data)
				points_out->collection.push_back(p);
		}
	}

	return points_out;
}
