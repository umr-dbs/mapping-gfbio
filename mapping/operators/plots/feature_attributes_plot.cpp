#include "operators/operator.h"
#include "util/make_unique.h"
#include "datatypes/plots/xygraph.h"

#include <string>
#include <json/json.h>
#include <limits>
#include <vector>
#include <array>
#include <cmath>
#include "datatypes/pointcollection.h"

/**
 * Operator that plots feature attributes
 * It currently only support 2 or 3 attributes
 *
 * Parameters:
 * - attributeName: list of attributes to use for the plot
 */
class FeatureAttributesPlotOperator: public GenericOperator {
	public:
		FeatureAttributesPlotOperator(int sourcecounts[], GenericOperator *sources[],	Json::Value &params);
		virtual ~FeatureAttributesPlotOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);

	private:
		std::vector<std::string> attributeNames;

#ifndef MAPPING_OPERATOR_STUBS
		template<std::size_t size>
			std::unique_ptr<GenericPlot> createXYGraph(PointCollection& points);
#endif
};

FeatureAttributesPlotOperator::FeatureAttributesPlotOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	Json::Value inputNames = params.get("attributeNames", Json::Value(Json::arrayValue));
	for (Json::ArrayIndex index = 0; index < inputNames.size(); ++index) {
		attributeNames.push_back(inputNames.get(index, "raster").asString());
	}

	if(attributeNames.size() < 2) {
		throw ArgumentException("PointsMetadataToGraph: There must be more than 1 argument.");
	}
	if(attributeNames.size() > 3) {
		throw ArgumentException("PointsMetadataToGraph: There must not be more than 3 arguments.");
	}
}

FeatureAttributesPlotOperator::~FeatureAttributesPlotOperator() {}
REGISTER_OPERATOR(FeatureAttributesPlotOperator, "feature_attributes_plot");

void FeatureAttributesPlotOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{\"attributeNames\":[";
	for(auto& name : attributeNames) {
		stream << "\"" << name << "\",";
	}
	stream.seekp(((long) stream.tellp()) - 1); // remove last comma
	stream << "]}";
}


#ifndef MAPPING_OPERATOR_STUBS
template<std::size_t size>
auto FeatureAttributesPlotOperator::createXYGraph(PointCollection& points) -> std::unique_ptr<GenericPlot> {
	auto xyGraph = make_unique<XYGraph<size>>();

	for (size_t featureIndex = 0; featureIndex < points.getFeatureCount(); ++featureIndex) {
		std::array<double, size> value;
		bool hasData = true;

		for (size_t valueIndex = 0; valueIndex < size; ++valueIndex) {
			value[valueIndex] = points.feature_attributes.numeric(attributeNames[valueIndex]).get(featureIndex);

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

	xyGraph->sort();
	return std::move(xyGraph);
}

std::unique_ptr<GenericPlot> FeatureAttributesPlotOperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto points = getPointCollectionFromSource(0, QueryRectangle(rect, rect, QueryResolution::none()), profiler);

	// TODO: GENERALIZE
	if (attributeNames.size() == 2) {
		return createXYGraph<2>(*points);
	} else  {
		return createXYGraph<3>(*points);
	}

}
#endif
