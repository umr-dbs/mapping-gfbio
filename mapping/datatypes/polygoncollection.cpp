#include "util/exceptions.h"

#include <sstream>
#include "polygoncollection.h"
#include "util/make_unique.h"

template<typename T>
std::unique_ptr<PolygonCollection> filter(PolygonCollection *in, const std::vector<T> &keep) {
	size_t count = in->getFeatureCount();
	if (keep.size() != count) {
		std::ostringstream msg;
		msg << "PolygonCollection::filter(): size of filter does not match (" << keep.size() << " != " << count << ")";
		throw ArgumentException(msg.str());
	}

	size_t kept_count = 0;
	for (size_t idx=0;idx<count;idx++) {
		if (keep[idx])
			kept_count++;
	}

	auto out = std::make_unique<PolygonCollection>(in->stref);
	out->start_feature.reserve(kept_count);
	// copy global metadata
	out->global_md_string = in->global_md_string;
	out->global_md_value = in->global_md_value;

	// copy features
	for (auto feature : *in) {
		if (keep[feature]) {
			//copy polygons
			for(auto polygon : feature){
				for(auto ring : polygon){
					//copy coordinates
					for (auto & c : ring) {
						out->addCoordinate(c.x, c.y);
					}
					out->finishRing();
				}
				out->finishPolygon();
			}
			out->finishFeature();
		}
	}

	// copy local MD
	for (auto &keyValue : in->local_md_string) {
		const auto &vec_in = in->local_md_string.getVector(keyValue.first);
		auto &vec_out = out->local_md_string.addEmptyVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}

	for (auto &keyValue : in->local_md_value) {
		const auto &vec_in = in->local_md_value.getVector(keyValue.first);
		auto &vec_out = out->local_md_value.addEmptyVector(keyValue.first, kept_count);
		for (size_t idx=0;idx<count;idx++) {
			if (keep[idx])
				vec_out.push_back(vec_in[idx]);
		}
	}
	// copy time arrays
	if (in->hasTime()) {
		out->time_start.reserve(kept_count);
		out->time_end.reserve(kept_count);
		for (auto i=0;i<count;i++) {
			for (size_t idx=0;idx<count;idx++) {
				if (keep[idx]) {
					out->time_start.push_back(in->time_start[idx]);
					out->time_end.push_back(in->time_end[idx]);
				}
			}
		}
	}

	return out;
}

std::unique_ptr<PolygonCollection> PolygonCollection::filter(const std::vector<bool> &keep) {
	return ::filter<bool>(this, keep);
}

std::unique_ptr<PolygonCollection> PolygonCollection::filter(const std::vector<char> &keep) {
	return ::filter<char>(this, keep);
}

std::string PolygonCollection::toGeoJSON(bool displayMetadata) const {
	//TODO: implement inclusion of metadata
	//TODO: output MultiPolygon that consists of single polygon as Polygon?

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
