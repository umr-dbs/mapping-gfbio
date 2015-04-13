#include "multipolygoncollection.h"

#include <sstream>

std::string MultiPolygonCollection::toGeoJSON(bool displayMetadata) {
	//TODO: implement inclusion of metadata

	std::ostringstream json;
	json << std::fixed;

	json << "{\"type\":\"FeatureCollection\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";


	for(size_t featureIndex = 0; featureIndex < startFeature.size(); ++featureIndex){
		json << "{\"type\":\"Feature\",\"geometry\":{\"type\": \"MultiPolygon\", \"coordinates\": [";

		for(size_t polygonIndex = startFeature[featureIndex]; polygonIndex < stopFeature(featureIndex); ++polygonIndex){
			json << "[";

			for (size_t ringIndex = startPolygon[polygonIndex]; ringIndex < stopPolygon(polygonIndex); ++ringIndex){
				json << "[";

				for(size_t pointIndex = startRing[ringIndex]; pointIndex < stopRing(ringIndex); ++pointIndex){
					json << "[" << points[pointIndex].x << ", " << points[pointIndex].y << "],";
				}

				json.seekp(((long)json.tellp()) - 1);

				json << "],";
			}

			json.seekp(((long)json.tellp()) - 1);

			json << "],";
		}
		json.seekp(((long)json.tellp()) - 1);

		json << "]}},";
	}
	json.seekp(((long)json.tellp()) - 1);

	json << "]}";

	return json.str();

}

std::string MultiPolygonCollection::toCSV() {
	 //TODO: implement
	return "";
}
