#include "raster/exceptions.h"

#include <sstream>
#include "polygoncollection.h"

std::string PolygonCollection::toGeoJSON(bool displayMetadata) const {
	//TODO: implement inclusion of metadata

	std::ostringstream json;
	json << std::fixed;

	json << "{\"type\":\"FeatureCollection\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";

	for (auto feature: *this) {
		json << "{\"type\":\"Feature\",\"geometry\":{\"type\": \"MultiPolygon\", \"coordinates\": [";

		for (auto polygon : feature) {
			json << "[";

			for (auto ring : polygon) {
				json << "[";

				for (auto & coordinate : ring) {
					json << "[" << coordinate.x << ", " << coordinate.y << "],";
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

std::string PolygonCollection::toCSV() const {
	 //TODO: implement
	return "";
}

bool PolygonCollection::isSimple() const {
	return getFeatureCount() == (start_polygon.size() - 1);
}

void PolygonCollection::addCoordinate(double x, double y){
	coordinates.push_back(Coordinate(x, y));
}

//TODO: check that ring is closed
size_t PolygonCollection::finishRing(){
	if(start_ring.back() >= coordinates.size()){
		throw FeatureException("Tried to finish ring with 0 coordinates");
	}
	start_ring.push_back(coordinates.size());
	return start_ring.size() -2;
}

size_t PolygonCollection::finishPolygon(){
	if(start_polygon.back() >= start_ring.size()){
		throw FeatureException("Tried to finish polygon with 0 rings");
	}
	start_polygon.push_back(start_ring.size() - 1);
	return start_polygon.size() -2;
}

size_t PolygonCollection::finishFeature(){
	if(start_feature.back() >= start_polygon.size()){
		throw FeatureException("Tried to finish feature with 0 polygons");
	}
	start_feature.push_back(start_polygon.size() - 1);
	return start_feature.size() -2;
}

std::string PolygonCollection::getAsString(){
	std::ostringstream string;

	string << "points" << std::endl;
	for(auto p = coordinates.begin(); p !=coordinates.end(); ++p){
		string << (*p).x << "," << (*p).y << ' ';
	}

	string << std::endl;
	string << "rings" << std::endl;
	for(auto p =start_ring.begin(); p !=start_ring.end(); ++p){
		string << *p << ' ';
	}

	string << std::endl;
	string << "polygons" << std::endl;
	for(auto p = start_polygon.begin(); p != start_polygon.end(); ++p){
		string << *p << ' ';
	}

	string << std::endl;
	string << "features" << std::endl;
	for(auto p = start_feature.begin(); p != start_feature.end(); ++p){
		string << *p << ' ';
	}

	return string.str();
}
