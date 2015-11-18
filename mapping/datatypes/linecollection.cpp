#include "linecollection.h"
#include <sstream>
#include "util/make_unique.h"
#include "util/binarystream.h"


LineCollection::LineCollection(BinaryStream &stream) : SimpleFeatureCollection(stream) {
	bool hasTime;
	stream.read(&hasTime);

	size_t featureCount;
	stream.read(&featureCount);
	start_feature.reserve(featureCount);

	size_t lineCount;
	stream.read(&lineCount);
	start_line.reserve(lineCount);

	size_t coordinateCount;
	stream.read(&coordinateCount);
	coordinates.reserve(coordinateCount);

	global_attributes.fromStream(stream);
	local_md_string.fromStream(stream);
	local_md_value.fromStream(stream);

	if (hasTime) {
		time_start.reserve(featureCount);
		time_end.reserve(featureCount);
		double time;
		for (size_t i = 0; i < featureCount; i++) {
			stream.read(&time);
			time_start.push_back(time);
		}
		for (size_t i = 0; i < featureCount; i++) {
			stream.read(&time);
			time_end.push_back(time);
		}
	}

	uint32_t offset;
	for (size_t i = 0; i < featureCount; i++) {
		stream.read(&offset);
		start_feature.push_back(offset);
	}

	for (size_t i = 0; i < lineCount; i++) {
		stream.read(&offset);
		start_line.push_back(offset);
	}

	for (size_t i = 0; i < coordinateCount; i++) {
		coordinates.push_back(Coordinate(stream));
	}
}

void LineCollection::toStream(BinaryStream &stream) {
	stream.write(stref);
	stream.write(hasTime());

	size_t featureCount = start_feature.size();
	stream.write(featureCount);
	size_t lineCount = start_line.size();
	stream.write(lineCount);
	size_t coordinateCount = coordinates.size();
	stream.write(coordinateCount);


	stream.write(global_attributes);
	stream.write(local_md_string);
	stream.write(local_md_value);

	if (hasTime()) {
		for (size_t i = 0; i < featureCount; i++) {
			stream.write(time_start[i]);
		}
		for (size_t i = 0; i < featureCount; i++) {
			stream.write(time_end[i]);
		}
	}

	for (size_t i = 0; i < featureCount; i++) {
		stream.write(start_feature[i]);
	}

	for (size_t i = 0; i < lineCount; i++) {
		stream.write(start_line[i]);
	}

	for (size_t i = 0; i < coordinateCount; i++) {
		coordinates[i].toStream(stream);
	}
}

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

	// copy global attributes
	out->global_attributes = in->global_attributes;

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

bool LineCollection::featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const{
	Coordinate rectP1 = Coordinate(x1, y1);
	Coordinate rectP2 = Coordinate(x2, y1);
	Coordinate rectP3 = Coordinate(x2, y2);
	Coordinate rectP4 = Coordinate(x1, y2);

	for(auto line : getFeatureReference(featureIndex)){
		for(int i = start_line[line.getLineIndex()]; i < start_line[line.getLineIndex()+1] - 1; ++i){
			const Coordinate& c1 = coordinates[i];
			const Coordinate& c2 = coordinates[i + 1];
			if((c1.x >= x1 && c1.x <= x2 && c1.y >= y1 && c1.y <= y2) ||
			   lineSegmentsIntersect(c1, c2, rectP1, rectP2) ||
			   lineSegmentsIntersect(c1, c2, rectP2, rectP3) ||
			   lineSegmentsIntersect(c1, c2, rectP3, rectP4) ||
			   lineSegmentsIntersect(c1, c2, rectP4, rectP1)){
				return true;
			}
		}
	}
	return false;
}

std::unique_ptr<LineCollection> LineCollection::filterByRectangleIntersection(double x1, double y1, double x2, double y2){
	std::vector<bool> keep(getFeatureCount());

	for(auto feature : *this){
		if(featureIntersectsRectangle(feature, x1, y1, x2, y2)){
			keep[feature] = true;
		}
	}

	return filter(keep);
}

std::unique_ptr<LineCollection> LineCollection::filterByRectangleIntersection(const SpatialReference& sref){
	return filterByRectangleIntersection(sref.x1, sref.y1, sref.x2, sref.y2);
}

void LineCollection::addCoordinate(double x, double y){
	coordinates.push_back(Coordinate(x, y));
}

size_t LineCollection::finishLine(){
	if(coordinates.size() < start_line.back() + 2){
		throw FeatureException("Tried to finish line with less than 2 coordinates");
	}
	start_line.push_back(coordinates.size());
	return start_line.size() -2;
}

size_t LineCollection::finishFeature(){
	if(start_line.size() == 1 || (start_feature.back() >= start_line.size())){
		throw FeatureException("Tried to finish feature with 0 lines");
	}

	start_feature.push_back(start_line.size() - 1);
	return start_feature.size() -2;
}

void LineCollection::featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const {
	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1)
		json << "{\"type\":\"LineString\",\"coordinates\":";
	else
		json << "{\"type\":\"MultiLineString\",\"coordinates\":[";

	for(auto line : feature){
		json << "[";
		for(auto& c : line){
			json << "[" << c.x << "," << c.y << "],";
		}
		json.seekp(((long)json.tellp()) - 1); //delete last ,
		json << "],";
	}
	json.seekp(((long)json.tellp()) - 1); //delete last ,

	if(feature.size() > 1)
		json << "]";
	json << "}";
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

SpatialReference LineCollection::getFeatureMBR(size_t featureIndex) const {
	return getFeatureReference(featureIndex).getMBR();
}

void LineCollection::validateSpecifics() const {
	if(start_line.back() != coordinates.size())
		throw FeatureException("Line not finished");

	if(start_feature.back() != start_line.size() - 1)
		throw FeatureException("Feature not finished");
}
