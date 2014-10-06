#include "raster/pointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "plot/xygraph.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <vector>
#include <array>
#include <cmath>

class PointsMetadataToGraph: public GenericOperator {
private:
	std::vector<std::string> names;

	template<std::size_t size>
		std::unique_ptr<GenericPlot> createXYGraph(PointCollection& points);

public:
	PointsMetadataToGraph(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
	virtual ~PointsMetadataToGraph();

	virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect);
};

PointsMetadataToGraph::PointsMetadataToGraph(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	Json::Value inputNames = params.get("names", Json::Value(Json::arrayValue));
	for (Json::ArrayIndex index = 0; index < inputNames.size(); ++index) {
		names.push_back(inputNames.get(index, "raster").asString());
	}
}

PointsMetadataToGraph::~PointsMetadataToGraph() {}
REGISTER_OPERATOR(PointsMetadataToGraph, "points_metadata_to_graph");

template<std::size_t size>
auto PointsMetadataToGraph::createXYGraph(PointCollection& points) -> std::unique_ptr<GenericPlot> {
	auto xygraph = std::make_unique<XYGraph<size>>();

	std::vector<bool> hasNoData;
	std::vector<double> noDataValue;

	for (std::string& name : names) {
		hasNoData.push_back(points.global_md_value.get(name + "_has_no_data"));
		noDataValue.push_back(points.global_md_value.get(name + "_no_data"));
	}

	size_t count = points.collection.size();
	for (size_t point_idx=0;point_idx<count;point_idx++) {
		std::array<double, size> value;
		bool hasData = true;

		for (size_t index = 0; index < size; ++index) {
			value[index] = points.local_md_value.get(point_idx, names[index]);

			if((hasNoData[index] && (std::fabs(value[index] - noDataValue[index]) < std::numeric_limits<double>::epsilon())) // no data
					|| std::isnan(value[index])) /* is NaN */ {
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

std::unique_ptr<GenericPlot> PointsMetadataToGraph::getPlot(const QueryRectangle &rect) {
	auto points = getPointsFromSource(0, rect);

	// TODO: GENERALIZE
	if(names.size() == 3) {
		return createXYGraph<3>(*points);
	} else {
		return createXYGraph<2>(*points);
	}

}
