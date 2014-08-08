#ifndef RASTER_XYGRAPH_H
#define RASTER_XYGRAPH_H

#include <vector>
#include <array>
#include <sstream>
#include <string>

#include "datavector.h"

template<std::size_t dimensions>
class XYGraph : public DataVector {
	public:
		XYGraph() : nodata_count(0) {};

		void addPoint(std::array<double, dimensions> point) { points.push_back(point); }
		void incNoData() { nodata_count++; }

		std::string toJSON() {
			std::stringstream buffer;
			buffer << "{\"type\": \"xygraph\", ";
			buffer << "\"metadata\": {\"dimensions\": " << dimensions << ", \"nodata\": " << nodata_count << ", \"numberOfPoints\": " << points.size() << "}, ";
			buffer << "\"data\": [";
			for(std::array<double, dimensions>& point : points) {
				buffer << "[";
				for(double& element : point) {
					buffer << element << ",";
				}
				buffer.unget();
				buffer << "],";

			}
			buffer.seekp(((long) buffer.tellp()) - 1);
			buffer << "]}";
			return buffer.str();
		}

	private:
		std::vector<std::array<double, dimensions>> points;
		std::size_t nodata_count;
};

#endif
