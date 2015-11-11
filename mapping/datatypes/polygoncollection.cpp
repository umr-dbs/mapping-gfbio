
#include "datatypes/polygoncollection.h"
#include "util/make_unique.h"
#include "util/hash.h"

#include <sstream>

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

	auto out = make_unique<PolygonCollection>(in->stref);
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

std::unique_ptr<PolygonCollection> PolygonCollection::filterByRectangleIntersection(double x1, double y1, double x2, double y2){
	std::vector<bool> keep(getFeatureCount());
	Coordinate rectP1 = Coordinate(x1, y1);
	Coordinate rectP2 = Coordinate(x2, y1);
	Coordinate rectP3 = Coordinate(x2, y2);
	Coordinate rectP4 = Coordinate(x1, y2);

	for(auto feature : *this){
		if(feature.contains(rectP1) || feature.contains(rectP2) ||
		   feature.contains(rectP3) || feature.contains(rectP4)){
			keep[feature] = true;
			continue;
		}
		for(auto polygon : feature){
			auto shellIndex = polygon.getRingReference(0).getRingIndex();
			for(int i = start_ring[shellIndex]; i < start_ring[shellIndex + 1] - 1; ++i){
				Coordinate& c1 = coordinates[i];
				Coordinate& c2 = coordinates[i +1];

				if((c1.x >= x1 && c1.x <= x2 && c1.y >= y1 && c1.y <= y2) ||
				   lineSegmentsIntersect(c1, c2, rectP1, rectP2) ||
				   lineSegmentsIntersect(c1, c2, rectP2, rectP3) ||
				   lineSegmentsIntersect(c1, c2, rectP3, rectP4) ||
				   lineSegmentsIntersect(c1, c2, rectP4, rectP1)){
					keep[feature] = true;
					break;
				}
			}

			if(keep[feature])
				break;
		}
	}

	return filter(keep);
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

std::string PolygonCollection::toCSV() const {
	 //TODO: implement
	return "";
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

std::string PolygonCollection::hash() {
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

void PolygonCollection::validateSpecifics() const {
	if(start_ring.back() != coordinates.size())
		throw FeatureException("Ring not finished");

	if(start_polygon.back() != start_ring.size() - 1)
		throw FeatureException("Polygon not finished");

	if(start_feature.back() != start_polygon.size() - 1)
		throw FeatureException("Feature not finished");
}

