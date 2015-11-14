
#include "simplefeaturecollection.h"
#include "util/exceptions.h"
#include "util/binarystream.h"
#include "util/hash.h"
#include "util/make_unique.h"

#include <sstream>
#include <iomanip>
#include <iostream>
#include <cmath>
#include <limits>
#include <json/json.h>

Coordinate::Coordinate(BinaryStream &stream) {
	stream.read(&x);
	stream.read(&y);
}
void Coordinate::toStream(BinaryStream &stream) const {
	stream.write(x);
	stream.write(y);
}

/**
 * Timestamps
 */
bool SimpleFeatureCollection::hasTime() const {
	return time_start.size() == getFeatureCount();
}

void SimpleFeatureCollection::addDefaultTimestamps() {
	addDefaultTimestamps(std::numeric_limits<double>::min(), std::numeric_limits<double>::max());
}

void SimpleFeatureCollection::addDefaultTimestamps(double min, double max) {
	if (hasTime())
		return;
	auto fcount = getFeatureCount();
	time_start.empty();
	time_start.resize(fcount, min);
	time_end.empty();
	time_end.resize(fcount, max);
}



/*
 * Validation
 */
void SimpleFeatureCollection::validate() const {
	auto fcount = getFeatureCount();
	if (time_start.size() > 0 || time_end.size() > 0) {
		if (time_start.size() != fcount || time_end.size() != fcount)
			throw ArgumentException("SimpleFeatureCollection: size of the time-arrays doesn't match feature count");
	}

	for (auto key : local_md_string.getKeys()) {
		if (local_md_string.getVector(key).size() != fcount)
			throw ArgumentException(concat("SimpleFeatureCollection: size of string attribute vector \"", key, "\" doesn't match feature count"));
	}

	for (auto key : local_md_value.getKeys()) {
		if (local_md_value.getVector(key).size() != fcount)
			throw ArgumentException(concat("SimpleFeatureCollection: size of value attribute vector \"", key, "\" doesn't match feature count"));
	}

	validateSpecifics();
}


/*
 * Export
 */
std::string SimpleFeatureCollection::toGeoJSON(bool displayMetadata) const {
	std::ostringstream json;
	json << std::fixed; // std::setprecision(4);

	json << "{\"type\":\"FeatureCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:" << (int) stref.epsg <<"\"}},\"features\":[";

	auto value_keys = local_md_value.getKeys();
	auto string_keys = local_md_string.getKeys();
	bool isSimpleCollection = isSimple();
	for (size_t feature = 0; feature < getFeatureCount(); ++feature) {
		json << "{\"type\":\"Feature\",\"geometry\":";
		featureToGeoJSONGeometry(feature, json);

		if(displayMetadata && (string_keys.size() > 0 || value_keys.size() > 0 || hasTime())){
			json << ",\"properties\":{";

			//TODO: handle missing metadata values
			for (auto &key : string_keys) {
				json << "\"" << key << "\":" << Json::valueToQuotedString(local_md_string.get(feature, key).c_str()) << ",";
			}

			for (auto &key : value_keys) {
				double value = local_md_value.get(feature, key);
				json << "\"" << key << "\":";
				if (std::isfinite(value)) {
					json << value;
				}
				else {
					json << "null";
				}

				json << ",";
			}

			if (hasTime()) {
				json << "\"time_start\":" << time_start[feature] << ",\"time_end\":" << time_end[feature] << ",";
			}

			json.seekp(((long) json.tellp()) - 1); // delete last ,
			json << "}";
		}
		json << "},";

	}

	if(getFeatureCount() > 0)
		json.seekp(((long) json.tellp()) - 1); // delete last ,
	json << "]}";

	return json.str();
}


std::string SimpleFeatureCollection::toWKT() const {
	std::ostringstream wkt;

	wkt << "GEOMETRYCOLLECTION(";

	for(size_t i = 0; i < getFeatureCount(); ++i){
		featureToWKT(i, wkt);
		wkt << ",";
	}
	if(getFeatureCount() > 0)
		wkt.seekp(((long) wkt.tellp()) - 1); // delete last ,

	wkt << ")";

	return wkt.str();
}

std::string SimpleFeatureCollection::featureToWKT(size_t featureIndex) const{
	std::ostringstream wkt;
	featureToWKT(featureIndex, wkt);
	return wkt.str();
}

std::string SimpleFeatureCollection::toARFF(std::string layerName) const {
	std::ostringstream arff;

	arff << "@RELATION " << layerName << std::endl << std::endl;

	arff << "@ATTRIBUTE wkt STRING" << std::endl;

	if (hasTime()){
		arff << "@ATTRIBUTE time_start DATE" << std::endl;
		arff << "@ATTRIBUTE time_end DATE" << std::endl;
	}

	auto string_keys = local_md_string.getKeys();
	auto value_keys = local_md_value.getKeys();

	for(auto &key : string_keys) {
		arff << "@ATTRIBUTE" << " " << key << " " << "STRING" << std::endl;
	}
	for(auto &key : value_keys) {
		arff << "@ATTRIBUTE" << " " << key << " " << "NUMERIC" << std::endl;
	}

	arff << std::endl;
	arff << "@DATA" << std::endl;

	for (size_t featureIndex = 0; featureIndex < getFeatureCount(); ++featureIndex) {
		arff << "\"";
		featureToWKT(featureIndex, arff);
		arff << "\"";
		if (hasTime()){
			arff << "," << "\"" << stref.toIsoString(time_start[featureIndex]) << "\"" << ","
					 << "\"" << stref.toIsoString(time_end[featureIndex]) << "\"";
		}

		//TODO: handle missing metadata values
		for(auto &key : string_keys) {
			arff << ",\"" << local_md_string.get(featureIndex, key) << "\"";
		}
		for(auto &key : value_keys) {
			arff << "," << local_md_value.get(featureIndex, key);
		}
		arff << std::endl;
	}

	return arff.str();
}

SpatialReference SimpleFeatureCollection::calculateMBR(size_t coordinateIndexStart, size_t coordinateIndexStop) const {
	if(coordinateIndexStart >= coordinates.size() || coordinateIndexStop > coordinates.size() || coordinateIndexStart >= coordinateIndexStop)
		throw ArgumentException("Invalid start/stop index for coordinates");

	SpatialReference reference(stref.epsg);

	const Coordinate& c0 = coordinates[coordinateIndexStart];
	reference.x1 = c0.x;
	reference.x2 = c0.x;
	reference.y1 = c0.y;
	reference.y2 = c0.y;

	for(size_t i = coordinateIndexStart + 1; i < coordinateIndexStop; ++i){
		const Coordinate& c = coordinates[i];

		if(c.x < reference.x1)
			reference.x1 = c.x;
		else if(c.x > reference.x2)
			reference.x2 = c.x;

		if(c.y < reference.y1)
			reference.y1 = c.y;
		else if(c.y > reference.y2)
			reference.y2 = c.y;
	}

	return reference;
}

SpatialReference SimpleFeatureCollection::getCollectionMBR() const {
	return calculateMBR(0, coordinates.size());
}

/**
 * check if Coordinate c is on line given by Coordinate p1, p2
 * @param p1 start of line
 * @param p2 end of line
 * @param c coordinate to check, must be collinear to p1, p2
 * @return true if c is on line segment
 */
bool onSegment(const Coordinate& p1, const Coordinate& p2, const Coordinate& c)
{
    if (c.x <= std::max(p1.x, p2.x) && c.x >= std::min(p1.x, p2.x) &&
        c.y <= std::max(p1.y, p2.y) && c.y >= std::min(p1.y, p2.y))
       return true;

    return false;
}
enum Orientation {
	LEFT, RIGHT, ON
};

/**
 * calculate orientation of coordinate c with respect to line from p1 to p2
 * @param p1 start of line
 * @param p2 end of line
 * @param c coordinate to check
 * @return the orientation of c
 */
Orientation orientation(const Coordinate& p1, const Coordinate& p2, const Coordinate& c) {
    int val = (p2.y - p1.y) * (c.x - p2.x) -
              (p2.x - p1.x) * (c.y - p2.y);

    if (val == 0) return ON;  // colinear

    return (val > 0)? RIGHT: LEFT;
}

bool SimpleFeatureCollection::lineSegmentsIntersect(const Coordinate& p1, const Coordinate& p2, const Coordinate& p3, const Coordinate& p4) const {
	// idea from: http://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/

	//TODO: quick check if bounding box intersects to exit early?

	Orientation o1 = orientation(p1, p2, p3);
	Orientation o2 = orientation(p1, p2, p4);
	Orientation o3 = orientation(p3, p4, p1);
	Orientation o4 = orientation(p3, p4, p2);

	// General case
	if (o1 != o2 && o3 != o4)
		return true;

	// Special Cases
	// p1, p2 and p3 are collinear and p3 lies on segment p1p2
	if (o1 == ON && onSegment(p1, p2, p3)) return true;

	// p1, p2 and p3 are collinear and p4 lies on segment p1p2
	if (o2 == ON && onSegment(p1, p2, p4)) return true;

	// p3, p4 and p1 are colinear and p1 lies on segment p3p4
	if (o3 == ON && onSegment(p3, p4, p1)) return true;

	 // p3, p4 and p2 are collinear and p2 lies on segment p3p4
	if (o4 == ON && onSegment(p3, p4, p2)) return true;

	return false;
}

bool SimpleFeatureCollection::featureIntersectsRectangle(size_t featureIndex, const SpatialReference& sref) const{
	return featureIntersectsRectangle(featureIndex, sref.x1, sref.y1, sref.x2, sref.y2);
}

