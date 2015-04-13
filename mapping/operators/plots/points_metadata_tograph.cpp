#include "datatypes/multipointcollection.h"
#include "operators/operator.h"
#include "util/make_unique.h"
#include "datatypes/plots/xygraph.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <vector>
#include <array>
#include <cmath>

class PointsMetadataToGraph: public GenericOperator {
	public:
		PointsMetadataToGraph(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
		virtual ~PointsMetadataToGraph();

		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);

	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::vector<std::string> names;

		template<std::size_t size>
			std::unique_ptr<GenericPlot> createXYGraph(MultiPointCollection& points);
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

void PointsMetadataToGraph::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"parameterNames\":[";
	for(auto& name : names) {
		stream << "\"" << name << "\",";
	}
	stream.seekp(((long) stream.tellp()) - 1); // remove last comma
	stream << "]";
}

template<std::size_t size>
auto PointsMetadataToGraph::createXYGraph(MultiPointCollection& points) -> std::unique_ptr<GenericPlot> {
	auto xyGraph = std::make_unique<XYGraph<size>>();

	for (size_t pointIndex = 0; pointIndex < points.points.size(); ++pointIndex) {
		std::array<double, size> value;
		bool hasData = true;

		for (size_t valueIndex = 0; valueIndex < size; ++valueIndex) {
			value[valueIndex] = points.local_md_value.get(pointIndex, names[valueIndex]);

			if(std::isnan(value[valueIndex])) {
				hasData = false;
				break;
			}
		}

		if(hasData) {
			xyGraph->addPoint(value);
		} else {
			xyGraph->incNoData();
		}
	}

	return std::move(xyGraph);
}

std::unique_ptr<GenericPlot> PointsMetadataToGraph::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getMultiPointCollectionFromSource(0, rect, profiler);

	// TODO: GENERALIZE
	if(names.size() == 3) {
		return createXYGraph<3>(*points);
	} else {
		return createXYGraph<2>(*points);
	}

}
