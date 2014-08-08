#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "raster/xygraph.h"

#include <string>
#include <json/json.h>
#include <limits>

class PointsFilterByRangeOperator: public GenericOperator {
private:
	std::string nameX, nameY;

public:
	PointsFilterByRangeOperator(int sourcecount, GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsFilterByRangeOperator();

	virtual std::unique_ptr<DataVector> getDataVector(const QueryRectangle &rect);
};

PointsFilterByRangeOperator::PointsFilterByRangeOperator(int sourcecount,	GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::DATAVECTOR, sourcecount, sources) {
	assumeSources(1);

	nameX = params.get("nameX", "raster").asString();
	nameY = params.get("nameY", "raster").asString();
}

PointsFilterByRangeOperator::~PointsFilterByRangeOperator() {}
REGISTER_OPERATOR(PointsFilterByRangeOperator, "points_filter_by_range");

std::unique_ptr<DataVector> PointsFilterByRangeOperator::getDataVector(const QueryRectangle &rect) {
	auto points = sources[0]->getPoints(rect);
	auto xygraph = std::make_unique<XYGraph<2>>();

	double raster_no_data_X = points->getGlobalMDValue(nameX + "_no_data");
	bool raster_has_no_data_X = points->getGlobalMDValue(nameX + "_has_no_data");

	double raster_no_data_Y = points->getGlobalMDValue(nameY + "_no_data");
	bool raster_has_no_data_Y = points->getGlobalMDValue(nameY + "_has_no_data");

	for (Point &point : points->collection) {
		double valueX = points->getLocalMDValue(point, nameX);
		double valueY = points->getLocalMDValue(point, nameY);

		if ((raster_has_no_data_X && valueX == raster_no_data_X) || (raster_has_no_data_Y && valueX == raster_no_data_Y)) {
			xygraph->incNoData();
		} else {
			xygraph->addPoint({{valueX, valueY}});
		}
	}

	return std::unique_ptr<DataVector>(std::move(xygraph));
}
