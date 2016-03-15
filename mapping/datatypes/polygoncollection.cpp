
#include "datatypes/polygoncollection.h"
#include "util/make_unique.h"
#include "util/hash.h"
#include "util/binarystream.h"

#include <sstream>


std::unique_ptr<PolygonCollection> PolygonCollection::clone() const {
	auto copy = make_unique<PolygonCollection>(stref);
	copy->global_attributes = global_attributes;
	copy->feature_attributes = feature_attributes.clone();
	copy->coordinates = coordinates;
	copy->time = time;
	copy->start_ring = start_ring;
	copy->start_polygon = start_polygon;
	copy->start_feature = start_feature;
	return copy;
}


PolygonCollection::PolygonCollection(BinaryReadBuffer &buffer) : SimpleFeatureCollection(SpatioTemporalReference(buffer)) {
	auto hasTime = buffer.read<bool>();

	auto featureCount = buffer.read<size_t>();
	start_feature.reserve(featureCount);

	auto polygonCount = buffer.read<size_t>();
	start_polygon.reserve(polygonCount);

	auto ringCount = buffer.read<size_t>();
	start_ring.reserve(ringCount);

	auto coordinateCount = buffer.read<size_t>();
	coordinates.reserve(coordinateCount);

	global_attributes.deserialize(buffer);
	feature_attributes.deserialize(buffer);

	if (hasTime) {
		time.reserve(featureCount);
		for (size_t i = 0; i < featureCount; i++) {
			time.push_back(TimeInterval(buffer));
		}
	}

	uint32_t offset;
	for (size_t i = 0; i < featureCount; i++) {
		buffer.read(&offset);
		start_feature.push_back(offset);
	}

	for (size_t i = 0; i < polygonCount; i++) {
		buffer.read(&offset);
		start_polygon.push_back(offset);
	}

	for (size_t i = 0; i < ringCount; i++) {
		buffer.read(&offset);
		start_ring.push_back(offset);
	}

	for (size_t i = 0; i < coordinateCount; i++) {
		coordinates.push_back(Coordinate(buffer));
	}
}

void PolygonCollection::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	buffer << stref;
	buffer << hasTime();

	size_t featureCount = start_feature.size();
	size_t polygonCount = start_polygon.size();
	size_t ringCount = start_ring.size();
	size_t coordinateCount = coordinates.size();
	buffer << featureCount << polygonCount << ringCount << coordinateCount;

	buffer.write(global_attributes, is_persistent_memory);
	buffer.write(feature_attributes, is_persistent_memory);

	if (hasTime()) {
		for (size_t i = 0; i < featureCount; i++) {
			buffer << time[i];
		}
	}

	for (size_t i = 0; i < featureCount; i++) {
		buffer << start_feature[i];
	}

	for (size_t i = 0; i < polygonCount; i++) {
		buffer << start_polygon[i];
	}

	for (size_t i = 0; i < ringCount; i++) {
		buffer << start_ring[i];
	}

	for (size_t i = 0; i < coordinateCount; i++) {
		buffer << coordinates[i];
	}
}


template<typename T>
std::unique_ptr<PolygonCollection> filter(const PolygonCollection &in, const std::vector<T> &keep, size_t kept_count) {
	size_t count = in.getFeatureCount();
	if (keep.size() != count)
		throw ArgumentException(concat("PolygonCollection::filter(): size of filter does not match (", keep.size(), " != ", count, ")"));

	auto out = make_unique<PolygonCollection>(in.stref);
	out->start_feature.reserve(kept_count);

	// copy global attributes
	out->global_attributes = in.global_attributes;

	// copy features
	for (auto feature : in) {
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

	// copy feature attributes
	out->feature_attributes = in.feature_attributes.filter(keep, kept_count);

	// copy time arrays
	if (in.hasTime()) {
		out->time.reserve(kept_count);
		for (size_t idx = 0; idx < count; idx++) {
			if (keep[idx]) {
				out->time.push_back(in.time[idx]);
			}
		}
	}

	return out;
}

std::unique_ptr<PolygonCollection> PolygonCollection::filter(const std::vector<bool> &keep) const {
	size_t kept_count = calculate_kept_count(keep);
	return ::filter<bool>(*this, keep, kept_count);
}

std::unique_ptr<PolygonCollection> PolygonCollection::filter(const std::vector<char> &keep) const {
	size_t kept_count = calculate_kept_count(keep);
	return ::filter<char>(*this, keep, kept_count);
}

void PolygonCollection::filterInPlace(const std::vector<bool> &keep) {
	auto kept_count = calculate_kept_count(keep);
	if (kept_count == getFeatureCount())
		return;
	auto other = ::filter<bool>(*this, keep, kept_count);
	*this = std::move(*other);
}

void PolygonCollection::filterInPlace(const std::vector<char> &keep) {
	auto kept_count = calculate_kept_count(keep);
	if (kept_count == getFeatureCount())
		return;
	auto other = ::filter<char>(*this, keep, kept_count);
	*this = std::move(*other);
}

std::unique_ptr<PolygonCollection> PolygonCollection::filterBySpatioTemporalReferenceIntersection(const SpatioTemporalReference& stref) const{
	auto keep = getKeepVectorForFilterBySpatioTemporalReferenceIntersection(stref);
	auto filtered = filter(keep);
	filtered->replaceSTRef(stref);
	return filtered;
}

void PolygonCollection::filterBySpatioTemporalReferenceIntersectionInPlace(const SpatioTemporalReference& stref) {
	auto keep = getKeepVectorForFilterBySpatioTemporalReferenceIntersection(stref);
	replaceSTRef(stref);
	filterInPlace(keep);
}

bool PolygonCollection::featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const{
	Coordinate rectP1 = Coordinate(x1, y1);
	Coordinate rectP2 = Coordinate(x2, y1);
	Coordinate rectP3 = Coordinate(x2, y2);
	Coordinate rectP4 = Coordinate(x1, y2);

	auto feature = getFeatureReference(featureIndex);

	if(feature.contains(rectP1) || feature.contains(rectP2) ||
	   feature.contains(rectP3) || feature.contains(rectP4)){
		return true;
	}
	for(auto polygon : feature){
		size_t shellIndex = polygon.getRingReference(0).getRingIndex();
		for(int i = start_ring[shellIndex]; i < start_ring[shellIndex + 1] - 1; ++i){
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

void PolygonCollection::featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const {
	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1)
		json << "{\"type\":\"Polygon\",\"coordinates\":";
	else
		json << "{\"type\":\"MultiPolygon\",\"coordinates\":[";

	for(auto polygon : feature){
		json << "[";
		for(auto ring : polygon){
			json << "[";
			for(auto& c : ring){
				json << "[" << c.x << "," << c.y << "],";
			}
			json.seekp(((long)json.tellp()) - 1); //delete last ,
			json << "],";
		}
		json.seekp(((long)json.tellp()) - 1); //delete last ,
		json << "],";
	}
	json.seekp(((long)json.tellp()) - 1); //delete last ,

	if(feature.size() > 1)
		json << "]";
	json << "}";
}

void PolygonCollection::featureToWKT(size_t featureIndex, std::ostringstream& wkt) const {
	if(featureIndex >= getFeatureCount()){
		throw ArgumentException("featureIndex is greater than featureCount");
	}

	auto feature = getFeatureReference(featureIndex);

	if(feature.size() == 1) {
		wkt << "POLYGON(";
		for(auto ring : *feature.begin()){
			wkt << "(";
			for(auto& coordinate : ring){
				wkt << coordinate.x << " " << coordinate.y << ",";
			}
			wkt.seekp(((long)wkt.tellp()) - 1);
			wkt << "),";
		}
		wkt.seekp(((long)wkt.tellp()) - 1);
		wkt << ")";
	}
	else {
		wkt << "MULTIPOLYGON(";
		for(auto polygon : feature){
			wkt << "(";
			for(auto ring : polygon){
				wkt << "(";
				for(auto& coordinate : ring){
					wkt << coordinate.x << " " << coordinate.y << ",";
				}
				wkt.seekp(((long)wkt.tellp()) - 1);
				wkt << "),";
			}
			wkt.seekp(((long)wkt.tellp()) - 1);
			wkt << "),";
		}
		wkt.seekp(((long)wkt.tellp()) - 1);
		wkt << ")";
	}
}

std::string PolygonCollection::hash() const {
	// certainly not the most stable solution, but it has few lines of code..
	std::string serialized = toGeoJSON(true);

	return calculateHash((const unsigned char *) serialized.c_str(), (int) serialized.length()).asHex();
}

bool PolygonCollection::isSimple() const {
	return getFeatureCount() == (start_polygon.size() - 1);
}

void PolygonCollection::addCoordinate(double x, double y){
	coordinates.push_back(Coordinate(x, y));
}

size_t PolygonCollection::finishRing(){
	if(coordinates.size() < start_ring.back() + 4){
		throw FeatureException("Tried to finish ring with less than 3 vertices (4 coordinates)");
	}
	if(!(coordinates[coordinates.size() - 1].almostEquals(coordinates[start_ring.back()]))){
		throw FeatureException("Last coordinate of ring is not equal to the first one");
	}

	start_ring.push_back(coordinates.size());
	return start_ring.size() -2;
}

size_t PolygonCollection::finishPolygon(){
	if(start_ring.size() == 1 || start_polygon.back() >= start_ring.size()){
		throw FeatureException("Tried to finish polygon with 0 rings");
	}
	start_polygon.push_back(start_ring.size() - 1);
	return start_polygon.size() -2;
}

size_t PolygonCollection::finishFeature(){
	if(start_polygon.size() == 1 || start_feature.back() >= start_polygon.size()){
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


bool PolygonCollection::pointInRing(const Coordinate& coordinate, size_t coordinateIndexStart, size_t coordinateIndexStop) const {
	//Algorithm from http://alienryderflex.com/polygon/
	size_t numberOfCorners = coordinateIndexStop - coordinateIndexStart - 1;
	size_t i, j = numberOfCorners - 1;
	bool oddNodes = false;

	for (i=0; i < numberOfCorners; ++i) {
		const Coordinate& c_i = coordinates[coordinateIndexStart + i];
		const Coordinate& c_j = coordinates[coordinateIndexStart + j];

		if ((c_i.y < coordinate.y && c_j.y >= coordinate.y)
		||  (c_j.y < coordinate.y && c_i.y >= coordinate.y)) {
			if (c_i.x + (coordinate.y - c_i.y) / (c_j.y - c_i.y) * (c_j.x - c_i.x) < coordinate.x) {
				oddNodes=!oddNodes;
			}
		}
		j = i;
	}

	return oddNodes;
}

bool PolygonCollection::pointInCollection(Coordinate& coordinate) const {
	for(auto feature : *this){
		if(feature.contains(coordinate))
			return true;
	}

	return false;
}

SpatialReference PolygonCollection::getFeatureMBR(size_t featureIndex) const {
	return getFeatureReference(featureIndex).getMBR();
}

SpatialReference PolygonCollection::getCollectionMBR() const{
	//TODO: compute MBRs of outer rings of all polygons and then the MBR of these MBRs?
	return calculateMBR(0, coordinates.size());
}


/**
 * PointInCollectionBulkTester
 */

PolygonCollection::PointInCollectionBulkTester::PointInCollectionBulkTester(const PolygonCollection& polygonCollection) : polygonCollection(polygonCollection){
	performPrecalculation();
}

void PolygonCollection::PointInCollectionBulkTester::precalculateRing(size_t coordinateIndexStart, size_t coordinateIndexStop){
	//precalculate values to avoid redundant computation later on
	size_t numberOfCorners = coordinateIndexStop - coordinateIndexStart - 1;
	size_t i, j = numberOfCorners - 1;

	for(i=0; i < numberOfCorners; ++i) {
		const Coordinate& c_i = polygonCollection.coordinates[coordinateIndexStart + i];
		const Coordinate& c_j = polygonCollection.coordinates[coordinateIndexStart + j];

		if(c_j.y == c_i.y) {
			constants[coordinateIndexStart + i] = c_i.x;
			multiples[coordinateIndexStart + i] = 0;
		} else {
			constants[coordinateIndexStart + i] = c_i.x - (c_i.y * c_j.x) / (c_j.y - c_i.y) + (c_i.y * c_i.x) / (c_j.y - c_i.y);
			multiples[coordinateIndexStart + i] = (c_j.x - c_i.x) / (c_j.y - c_i.y);
		}
		j = i;
	}
}

void PolygonCollection::PointInCollectionBulkTester::performPrecalculation(){
	constants.reserve(polygonCollection.coordinates.size());
	multiples.reserve(polygonCollection.coordinates.size());

	for(auto feature : polygonCollection){
		for(auto polygon : feature){
			for(auto ring : polygon){
				precalculateRing(polygonCollection.start_ring[ring.getRingIndex()], polygonCollection.start_ring[ring.getRingIndex() + 1]);
			}
		}
	}
}

bool PolygonCollection::PointInCollectionBulkTester::pointInRing(const Coordinate& coordinate, size_t coordinateIndexStart, size_t coordinateIndexStop) const {
	//Algorithm from http://alienryderflex.com/polygon/
	size_t numberOfCorners = coordinateIndexStop - coordinateIndexStart - 1;
	size_t i, j = numberOfCorners - 1;
	bool oddNodes = false;

	for (i=0; i < numberOfCorners; ++i) {
		const Coordinate& c_i = polygonCollection.coordinates[coordinateIndexStart + i];
		const Coordinate& c_j = polygonCollection.coordinates[coordinateIndexStart + j];

		if ((c_i.y < coordinate.y && c_j.y >= coordinate.y)
		||  (c_j.y < coordinate.y && c_i.y >= coordinate.y)) {
			oddNodes ^= (coordinate.y * multiples[coordinateIndexStart + i] + constants[coordinateIndexStart + i] < coordinate.x);
		}
		j = i;
	}

	return oddNodes;
}

bool PolygonCollection::PointInCollectionBulkTester::pointInCollection(const Coordinate& coordinate) const {
	for(auto feature : polygonCollection){
		for(auto polygon : feature){
			bool contained = true;
			size_t ringIndex = 0;
			for(auto ring : polygon){
				if(ringIndex == 0){
					if(!pointInRing(coordinate, polygonCollection.start_ring[ring.getRingIndex()], polygonCollection.start_ring[ring.getRingIndex() + 1])){
						contained = false;
						break;
					}
				}
				else {
					if(pointInRing(coordinate, polygonCollection.start_ring[ring.getRingIndex()], polygonCollection.start_ring[ring.getRingIndex() + 1])){
						contained = false;
						break;
					}
				}

				++ringIndex;
			}
			if(contained)
				return true;
		}
	}

	return false;
}

std::vector<uint32_t> PolygonCollection::PointInCollectionBulkTester::polygonsContainingPoint(const Coordinate& coordinate) const {
	std::vector<uint32_t> result;
	for(auto feature : polygonCollection){
		for(auto polygon : feature){
			bool contained = true;
			size_t ringIndex = 0;
			for(auto ring : polygon){
				if(ringIndex == 0){
					if(!pointInRing(coordinate, polygonCollection.start_ring[ring.getRingIndex()], polygonCollection.start_ring[ring.getRingIndex() + 1])){
						contained = false;
						break;
					}
				}
				else {
					if(pointInRing(coordinate, polygonCollection.start_ring[ring.getRingIndex()], polygonCollection.start_ring[ring.getRingIndex() + 1])){
						contained = false;
						break;
					}
				}

				++ringIndex;
			}
			if(contained)
				result.push_back(feature);
		}
	}

	return result;
}

void PolygonCollection::validateSpecifics() const {
	if(start_ring.back() != coordinates.size())
		throw FeatureException("Ring not finished");

	if(start_polygon.back() != start_ring.size() - 1)
		throw FeatureException("Polygon not finished");

	if(start_feature.back() != start_polygon.size() - 1)
		throw FeatureException("Feature not finished");
}

void PolygonCollection::removeLastFeature(){
	bool isTime = hasTime();
	if(start_feature.back() == start_polygon.size() - 1  &&
			start_polygon.back() == start_ring.size() -1 &&
			start_ring.back() == coordinates.size()){
		start_feature.pop_back();
	}
	start_polygon.erase(start_polygon.begin() + start_feature.back() + 1, start_polygon.end());

	start_ring.erase(start_ring.begin() + start_polygon.back() + 1, start_ring.end());

	coordinates.erase(coordinates.begin() + start_ring.back(), coordinates.end());

	size_t featureCount = getFeatureCount();

	if(isTime) {
		time.resize(featureCount);
	}
	feature_attributes.resize(featureCount);
}
