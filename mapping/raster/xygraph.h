#ifndef RASTER_XYGRAPH_H
#define RASTER_XYGRAPH_H

#include <vector>
#include <array>
#include <sstream>
#include <string>
#include <algorithm>
#include <limits>

#include "datavector.h"

template<std::size_t dimensions>
class XYGraph : public DataVector {
	public:
		XYGraph() {
			rangeMin.fill(std::numeric_limits<double>::max());
			rangeMax.fill(std::numeric_limits<double>::min());
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

		auto sort() -> void { std::sort(points.begin(), points.end(), sortFunction); sorted = true;	}

		auto toJSON() -> std::string {
			if(!sorted)
				sort();

			std::stringstream buffer;
			buffer << "{\"type\": \"xygraph\", ";
			buffer << "\"metadata\": {\"dimensions\": " << dimensions << ", \"nodata\": " << nodata_count << ", \"numberOfPoints\": " << points.size() << ", \"range\": [";
			for (std::size_t index = 0; index < dimensions; ++index) {
				buffer << "[" << rangeMin[index] << "," << rangeMax[index] << "],";
			}
			buffer.seekp(((long) buffer.tellp()) - 1);
			buffer << "]}, " << "\"data\": [";
			for(std::array<double, dimensions>& point : points) {
				buffer << "[";
				for(double& element : point) {
					buffer << element << ",";
				}
				buffer.seekp(((long) buffer.tellp()) - 1);
				buffer << "],";

			}
			buffer.seekp(((long) buffer.tellp()) - 1);
			buffer << "]}";
			return buffer.str();
		}

	private:
		std::vector<std::array<double, dimensions>> points;
		std::size_t nodata_count{0};

		std::array<double, dimensions> rangeMin;
		std::array<double, dimensions> rangeMax;

		bool sorted{false};
		static auto sortFunction(const std::array<double, dimensions>& e1,	const std::array<double, dimensions>& e2) -> bool {
			for (std::size_t dimension = 0; dimension < dimensions; ++dimension) {
				double difference = e1[dimension] - e2[dimension];

				if (std::fabs(difference) < std::numeric_limits<double>::epsilon())
					continue;
				else if (difference < 0)
					return true;
				else
					return false;
			}

			return true;
		}
};

#endif
