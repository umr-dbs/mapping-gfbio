#include "linecollection.h"
#include <sstream>
#include "util/make_unique.h"
#include "util/binarystream.h"
#include "util/hash.h"


std::unique_ptr<LineCollection> LineCollection::clone() const {
	auto copy = make_unique<LineCollection>(stref);
	copy->global_attributes = global_attributes;
	copy->feature_attributes = feature_attributes.clone();
	copy->coordinates = coordinates;
	copy->time_start = time_start;
	copy->time_end = time_end;
	copy->start_line = start_line;
	copy->start_feature = start_feature;
	return copy;
}


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
	feature_attributes.fromStream(stream);

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

void LineCollection::toStream(BinaryStream &stream) const {
	stream.write(stref);
	stream.write(hasTime());

	size_t featureCount = start_feature.size();
	stream.write(featureCount);
	size_t lineCount = start_line.size();
	stream.write(lineCount);
	size_t coordinateCount = coordinates.size();
	stream.write(coordinateCount);


	stream.write(global_attributes);
	stream.write(feature_attributes);

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
std::unique_ptr<LineCollection> filter(const LineCollection &in, const std::vector<T> &keep, size_t kept_count) {
	size_t count = in.getFeatureCount();
	if (keep.size() != count) {
		throw ArgumentException(concat("LineCollection::filter(): size of filter does not match (", keep.size(), " != ",count, ")"));
	}

	auto out = make_unique<LineCollection>(in.stref);
	out->start_feature.reserve(kept_count);

	// copy global attributes
	out->global_attributes = in.global_attributes;

	// copy features
	for (auto feature : in) {
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

	// copy feature attributes
	out->feature_attributes = in.feature_attributes.filter(keep, kept_count);

	// copy time arrays
	if (in.hasTime()) {
		out->time_start.reserve(kept_count);
		out->time_end.reserve(kept_count);
		for (size_t idx = 0; idx < count; idx++) {
			if (keep[idx]) {
				out->time_start.push_back(in.time_start[idx]);
				out->time_end.push_back(in.time_end[idx]);
			}
		}
	}

	return out;
}

std::unique_ptr<LineCollection> LineCollection::filter(const std::vector<bool> &keep) const {
	auto kept_count = calculate_kept_count(keep);
	return ::filter<bool>(*this, keep, kept_count);
}

std::unique_ptr<LineCollection> LineCollection::filter(const std::vector<char> &keep) const {
	auto kept_count = calculate_kept_count(keep);
	return ::filter<char>(*this, keep, kept_count);
}

void LineCollection::filterInPlace(const std::vector<bool> &keep) {
	auto kept_count = calculate_kept_count(keep);
	if (kept_count == getFeatureCount())
		return;
	auto other = ::filter<bool>(*this, keep, kept_count);
	*this = std::move(*other);
}

void LineCollection::filterInPlace(const std::vector<char> &keep) {
	auto kept_count = calculate_kept_count(keep);
	if (kept_count == getFeatureCount())
		return;
	auto other = ::filter<char>(*this, keep, kept_count);
	*this = std::move(*other);
}

std::unique_ptr<LineCollection> LineCollection::filterBySpatioTemporalReferenceIntersection(const SpatioTemporalReference& stref) const{
	auto keep = getKeepVectorForFilterBySpatioTemporalReferenceIntersection(stref);
	auto filtered = filter(keep);
	filtered->replaceSTRef(stref);
	return filtered;
}

void LineCollection::filterBySpatioTemporalReferenceIntersectionInPlace(const SpatioTemporalReference& stref) {
	auto keep = getKeepVectorForFilterBySpatioTemporalReferenceIntersection(stref);
	replaceSTRef(stref);
	filterInPlace(keep);
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

std::string LineCollection::hash() const {
	// certainly not the most stable solution, but it has few lines of code..
	std::string serialized = toGeoJSON(true);

	return calculateHash((const unsigned char *) serialized.c_str(), (int) serialized.length()).asHex();
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
