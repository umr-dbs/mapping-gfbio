#include "multipolygoncollection.h"
#include "raster/exceptions.h"

#include <sstream>

std::string MultiPolygonCollection::toGeoJSON(bool displayMetadata) {
	//TODO: implement inclusion of metadata

	std::ostringstream json;
	json << std::fixed;

	json << "{\"type\":\"FeatureCollection\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";


	for(size_t featureIndex = 0; featureIndex < getFeatureCount(); ++featureIndex){
		json << "{\"type\":\"Feature\",\"geometry\":{\"type\": \"MultiPolygon\", \"coordinates\": [";

		for(size_t polygonIndex = start_feature[featureIndex]; polygonIndex < start_feature[featureIndex + 1]; ++polygonIndex){
			json << "[";

			for (size_t ringIndex = start_polygon[polygonIndex]; ringIndex < start_polygon[polygonIndex + 1]; ++ringIndex){
				json << "[";

				for(size_t pointIndex = start_ring[ringIndex]; pointIndex < start_ring[ringIndex + 1]; ++pointIndex){
					json << "[" << coordinates[pointIndex].x << ", " << coordinates[pointIndex].y << "],";
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

bool MultiPolygonCollection::isSimple(){
	return getFeatureCount() == (start_polygon.size() - 1);
}

void MultiPolygonCollection::addCoordinate(double x, double y){
	coordinates.push_back(Coordinate(x, y));
}

size_t MultiPolygonCollection::finishRing(){
	if(start_ring.back() >= coordinates.size()){
		throw FeatureException("Tried to finish ring with 0 coordinates");
	}
	start_ring.push_back(coordinates.size());
	return start_ring.size() -2;
}

size_t MultiPolygonCollection::finishPolygon(){
	if(start_polygon.back() >= start_ring.size()){
		throw FeatureException("Tried to finish polygon with 0 rings");
	}
	start_polygon.push_back(start_ring.size() - 1);
	return start_polygon.size() -2;
}

size_t MultiPolygonCollection::finishFeature(){
	if(start_feature.back() >= start_polygon.size()){
		throw FeatureException("Tried to finish feature with 0 polygons");
	}
	start_feature.push_back(start_polygon.size() - 1);
	return start_feature.size() -2;
}

std::string MultiPolygonCollection::getAsString(){
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
