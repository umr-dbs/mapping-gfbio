#ifndef RASTER_XYGRAPH_H
#define RASTER_XYGRAPH_H

#include <vector>
#include <array>
#include <sstream>
#include <string>
#include <algorithm>
#include <limits>

#include "datatypes/plot.h"
#include "util/make_unique.h"

/**
 * This plot outputs n dimensional numeric attribute vectors as JSON
 */
template<std::size_t dimensions>
class XYGraph : public GenericPlot {
	public:
		XYGraph() {
			rangeMin.fill(std::numeric_limits<double>::max());
			rangeMax.fill(std::numeric_limits<double>::min());
		};

		virtual ~XYGraph() {
		};

		auto addPoint(std::array<double, dimensions> point) -> void {
			points.push_back(point);
			sorted = false;

			for (std::size_t index = 0; index < dimensions; ++index) {
				if(point[index] < rangeMin[index])
					rangeMin[index] = point[index];
				if(point[index] > rangeMax[index])
					rangeMax[index] = point[index];
			}
		}
		auto incNoData() -> void { nodata_count++; }

		auto sort() -> void { std::sort(points.begin(), points.end()); sorted = true; }

		auto toJSON() const -> const std::string {
			if(!sorted)
				throw OperatorException("The points must be sorted before exporting them.");

			std::stringstream buffer;
			buffer << "{\"type\": \"xygraph\", ";
			buffer << "\"metadata\": {\"dimensions\": " << dimensions << ", \"nodata\": " << nodata_count << ", \"numberOfPoints\": " << points.size() << ", \"range\": [";
			for (std::size_t index = 0; index < dimensions; ++index) {
				buffer << "[" << rangeMin[index] << "," << rangeMax[index] << "],";
			}
			buffer.seekp(((long) buffer.tellp()) - 1);
			buffer << "]}, " << "\"data\": [";
			for(const std::array<double, dimensions>& point : points) {
				buffer << "[";
				for(const double& element : point) {
					buffer << element << ",";
				}
				buffer.seekp(((long) buffer.tellp()) - 1);
				buffer << "],";

			}
			buffer.seekp(((long) buffer.tellp()) - 1);
			buffer << "]}";
			return buffer.str();
		}

	auto clone() const -> std::unique_ptr<GenericPlot> {
		auto copy = make_unique<XYGraph>();

		copy->points = points;
		copy->nodata_count = nodata_count;
		copy->rangeMin = rangeMin;
		copy->rangeMax = rangeMax;
		copy->sorted = sorted;

		return std::unique_ptr<GenericPlot>(copy.release());
	}

	private:
		std::vector<std::array<double, dimensions>> points;
		std::size_t nodata_count{0};

		std::array<double, dimensions> rangeMin;
		std::array<double, dimensions> rangeMax;

		bool sorted{false};
};

#endif
