#include "linecollection.h"
#include <sstream>
#include "util/exceptions.h"
#include "util/make_unique.h"


template<typename T>
std::unique_ptr<LineCollection> filter(LineCollection *in, const std::vector<T> &keep) {
	size_t count = in->getFeatureCount();
	if (keep.size() != count) {
		std::ostringstream msg;
		msg << "LineCollection::filter(): size of filter does not match (" << keep.size() << " != " << count << ")";
		throw ArgumentException(msg.str());
	}

	size_t kept_count = 0;
	for (size_t idx=0;idx<count;idx++) {
		if (keep[idx])
			kept_count++;
	}

	auto out = make_unique<LineCollection>(in->stref);
	out->start_feature.reserve(kept_count);
	// copy global metadata
	out->global_md_string = in->global_md_string;
	out->global_md_value = in->global_md_value;

	// copy features
	for (auto feature : *in) {
		if (keep[feature]) {
			//copy lines
			for(auto line : feature){
				//copy coordinates
				for (auto & c : line) {
					out->addCoordinate(c.x, c.y);
				}
				out->finishLine();
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

std::unique_ptr<LineCollection> LineCollection::filter(const std::vector<bool> &keep) {
	return ::filter<bool>(this, keep);
}

std::unique_ptr<LineCollection> LineCollection::filter(const std::vector<char> &keep) {
	return ::filter<char>(this, keep);
}

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

void LineCollection::featureToWKT(size_t featureIndex, std::ostringstream& wkt) const {
	if(featureIndex >= getFeatureCount()){
		throw ArgumentException("featureIndex is greater than featureCount");
	}

	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1) {
		wkt << "LINESTRING(";
		for(auto& coordinate: *feature.begin()){
			wkt << coordinate.x << " " << coordinate.y << ",";
		}
		wkt.seekp(((long)wkt.tellp()) - 1);
		wkt << ")";
	}
	else {
		wkt << "MULTILINESTRING(";

		for(auto line : feature){
			wkt << "(";
			for(auto& coordinate: line){
				wkt << coordinate.x << " " << coordinate.y << ",";
			}
			wkt.seekp(((long)wkt.tellp()) - 1);
			wkt << "),";
		}
		wkt.seekp(((long)wkt.tellp()) - 1);
		wkt << ")";
	}
}

bool LineCollection::isSimple() const {
	return getFeatureCount() == (start_line.size() - 1);
}

SpatialReference LineCollection::mbr() const{
	return calculateMBR(0, coordinates.size());
}

SpatialReference LineCollection::featureMBR(size_t featureIndex) const{
	if(featureIndex >= getFeatureCount())
		throw ArgumentException("FeatureIndex >= FeatureCount");

	return calculateMBR(start_line[start_feature[featureIndex]], start_line[start_feature[featureIndex + 1]]);
}

SpatialReference LineCollection::lineMBR(size_t featureIndex, size_t lineIndex) const{
	if(featureIndex >= getFeatureCount())
		throw ArgumentException("FeatureIndex >= FeatureCount");

	if(lineIndex >= getFeatureReference(featureIndex).size()){
		throw ArgumentException("LineIndex >= FeatureSize");
	}

	return calculateMBR(start_line[start_feature[featureIndex] + lineIndex], start_line[start_feature[featureIndex] + lineIndex + 1]);
}
