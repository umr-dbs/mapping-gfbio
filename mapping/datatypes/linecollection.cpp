#include "linecollection.h"
#include <sstream>
#include "raster/exceptions.h"


void LineCollection::addCoordinate(double x, double y){
	coordinates.push_back(Coordinate(x, y));
}

//TODO: check if line is valid
size_t LineCollection::finishLine(){
	if(start_line.back() >= coordinates.size()){
		throw FeatureException("Tried to finish line with 0 coordinates");
	}
	start_line.push_back(coordinates.size());
	return start_line.size() -2;
}

size_t LineCollection::finishFeature(){
	if(start_feature.back() >= coordinates.size()){
		throw FeatureException("Tried to finish feature with 0 coordinates");
	}

	start_feature.push_back(start_line.size() - 1);
	return start_feature.size() -2;
}

std::string LineCollection::toGeoJSON(bool displayMetadata) const {
	//TODO: implement inclusion of metadata
	//TODO: output MultiLineString that consists of single Line as LineString?

	std::ostringstream json;
	json << std::fixed;

	json << "{\"type\":\"FeatureCollection\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";

	for(size_t featureIndex = 0; featureIndex < getFeatureCount(); ++featureIndex){
		json << "{\"type\":\"Feature\",\"geometry\":{\"type\": \"MultiLineString\", \"coordinates\": [";

		for(size_t lineIndex = start_feature[featureIndex]; lineIndex < start_feature[featureIndex + 1]; ++lineIndex){
			json << "[";

				for(size_t pointIndex = start_line[lineIndex]; pointIndex < start_line[lineIndex + 1]; ++pointIndex){
					json << "[" << coordinates[pointIndex].x << ", " << coordinates[pointIndex].y << "],";
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

std::string LineCollection::toCSV() const {
	 //TODO: implement
	return "";
}

bool LineCollection::isSimple() const {
	return getFeatureCount() == (start_line.size() - 1);
}
