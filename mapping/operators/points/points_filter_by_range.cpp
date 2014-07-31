#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "raster/histogram.h"

#include <string>
#include <json/json.h>
#include <limits>

class PointsFilterByRangeOperator: public GenericOperator {
private:
	std::string name;
	bool includeNoData;
	double rangeMin, rangeMax;

	void copyGlobalMetadata(PointCollection* pointsOld, PointCollection* pointsNew);
	void copyLocalMetadata(PointCollection* pointsOld, PointCollection* pointsNew, std::vector<std::string>* localMDStringKeys, std::vector<std::string>* localMDValueKeys, Point* pointOld, Point* pointNew);
public:
	PointsFilterByRangeOperator(int sourcecount, GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsFilterByRangeOperator();

	virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
};

PointsFilterByRangeOperator::PointsFilterByRangeOperator(int sourcecount,	GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::POINTS, sourcecount, sources) {
	assumeSources(1);

	name = params.get("name", "raster").asString();
	includeNoData = params.get("includeNoData", false).asBool();
	rangeMin = params.get("rangeMin", std::numeric_limits<double>::min()).asDouble();
	rangeMax = params.get("rangeMax", std::numeric_limits<double>::max()).asDouble();
}

PointsFilterByRangeOperator::~PointsFilterByRangeOperator() {
}
REGISTER_OPERATOR(PointsFilterByRangeOperator, "points_filter_by_range");

std::unique_ptr<PointCollection> PointsFilterByRangeOperator::getPoints(const QueryRectangle &rect) {
	auto pointsOld = sources[0]->getPoints(rect);
	auto pointsNew = std::make_unique<PointCollection>(pointsOld->epsg);

	PointCollectionMetadataCopier metadataCopier(*pointsOld, *pointsNew);
	metadataCopier.copyGlobalMetadata();

	double raster_no_data = pointsOld->getGlobalMDValue(name + "_no_data");
	bool raster_has_no_data = pointsOld->getGlobalMDValue(name + "_has_no_data");

	metadataCopier.initLocalMetadataFields();
	for (Point &pointOld : pointsOld->collection) {
		double value = pointsOld->getLocalMDValue(pointOld, name);
		bool copy = false;

		if (raster_has_no_data && value == raster_no_data) {
			copy = includeNoData;
		} else {
			copy = (value >= rangeMin && value <= rangeMax);
		}

		if(copy) {
			auto pointNew = pointsNew->addPoint(pointOld.x, pointOld.y);
			metadataCopier.copyLocalMetadata(pointOld, pointNew);
		}
	}

	return pointsNew;
}
