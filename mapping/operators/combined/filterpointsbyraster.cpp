
#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "raster/pointcollection.h"
#include "operators/operator.h"

#include <memory>


class FilterPointsByRaster : public GenericOperator {
	public:
	FilterPointsByRaster(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~FilterPointsByRaster();

		virtual PointCollection *getPoints(const QueryRectangle &rect);
};




FilterPointsByRaster::FilterPointsByRaster(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(2);
}

FilterPointsByRaster::~FilterPointsByRaster() {
}
REGISTER_OPERATOR(FilterPointsByRaster, "filterpointsbyraster");


PointCollection *FilterPointsByRaster::getPoints(const QueryRectangle &rect) {

	PointCollection *points = sources[0]->getPoints(rect);
	std::unique_ptr<PointCollection> points_guard(points);

	GenericRaster *raster = sources[1]->getRaster(rect);
	std::unique_ptr<GenericRaster> raster_guard(raster);
	raster->setRepresentation(GenericRaster::Representation::CPU);


	PointCollection *points_out = new PointCollection();
	std::unique_ptr<PointCollection> points_out_guard(points_out);

	double no_data = 0.0; // 0 is always considered "false"
	if (raster->valuemeta.has_no_data)
		no_data = raster->valuemeta.no_data;

	for (Point &p : points->collection) {
		double x = p.x, y = p.y;

		int px = floor(raster->rastermeta.WorldToPixelX(x));
		int py = floor(raster->rastermeta.WorldToPixelY(y));

		if (px >= 0 && py >= 0 && (size_t) px < raster->rastermeta.size[0] && (size_t) py < raster->rastermeta.size[1]) {
			double value = raster->getAsDouble(px, py);

			if (value != 0.0 && value != no_data)
				points_out->collection.push_back(p);
		}
	}

	return points_out_guard.release();
}
