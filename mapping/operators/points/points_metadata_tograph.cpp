#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "raster/xygraph.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <vector>
#include <array>

class PointsMetadataToGraph: public GenericOperator {
private:
	std::vector<std::string> names;

public:
	PointsMetadataToGraph(int sourcecount, GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsMetadataToGraph();

	virtual auto getDataVector(const QueryRectangle &rect) -> std::unique_ptr<DataVector>;
};

PointsMetadataToGraph::PointsMetadataToGraph(int sourcecount,	GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::DATAVECTOR, sourcecount, sources) {
	assumeSources(1);

	Json::Value inputNames =  params.get("names", Json::Value(Json::arrayValue));
	for (Json::ArrayIndex index = 0; index < inputNames.size(); ++index) {
		names.push_back(inputNames.get(index, "raster").asString());
	}
}

PointsMetadataToGraph::~PointsMetadataToGraph() {}
REGISTER_OPERATOR(PointsMetadataToGraph, "points_metadata_to_graph");

auto PointsMetadataToGraph::getDataVector(const QueryRectangle &rect) -> std::unique_ptr<DataVector> {
	auto points = sources[0]->getPoints(rect);


	// TODO: GENERALIZE
	if(names.size() == 2) {
		const std::size_t size = 2;

		auto xygraph = std::make_unique<XYGraph<size>>();

		std::vector<bool> hasNoData;
		std::vector<double> noDataValue;

		for (std::string& name : names) {
			hasNoData.push_back(points->getGlobalMDValue(name + "_has_no_data"));
			noDataValue.push_back(points->getGlobalMDValue(name + "_no_data"));
		}

		for (Point &point : points->collection) {
			std::array<double, size> value;
			bool hasData = true;

			for (std::size_t index = 0; index < names.size(); ++index) {
				value[index] = points->getLocalMDValue(point, names[index]);

				if(hasNoData[index] && (std::fabs(value[index] - noDataValue[index]) < std::numeric_limits<double>::epsilon())) {
					hasData = false;
					break;
				}
			}

			if(hasData) {
				xygraph->addPoint(value);
			} else {
				xygraph->incNoData();
			}
		}

		return std::move(xygraph);
	} else {
		const std::size_t size = 3;

		auto xygraph = std::make_unique<XYGraph<size>>();

		std::vector<bool> hasNoData;
		std::vector<double> noDataValue;

		for (std::string& name : names) {
			hasNoData.push_back(points->getGlobalMDValue(name + "_has_no_data"));
			noDataValue.push_back(points->getGlobalMDValue(name + "_no_data"));
		}

		for (Point &point : points->collection) {
			std::array<double, size> value;
			bool hasData = true;

			for (std::size_t index = 0; index < names.size(); ++index) {
				value[index] = points->getLocalMDValue(point, names[index]);

				if(hasNoData[index] && (std::fabs(value[index] - noDataValue[index]) < std::numeric_limits<double>::epsilon())) {
					hasData = false;
					break;
				}
			}

			if(hasData) {
				xygraph->addPoint(value);
			} else {
				xygraph->incNoData();
			}
		}

		return std::move(xygraph);
	}

}
