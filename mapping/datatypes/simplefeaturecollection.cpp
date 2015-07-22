
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

Coordinate::Coordinate(BinaryStream &stream) {
	stream.read(&x);
	stream.read(&y);
}
void Coordinate::toStream(BinaryStream &stream) {
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


/**
 * Global Metadata
 */
const std::string &SimpleFeatureCollection::getGlobalMDString(const std::string &key) const {
	return global_md_string.get(key);
}

double SimpleFeatureCollection::getGlobalMDValue(const std::string &key) const {
	return global_md_value.get(key);
}

DirectMetadata<std::string>* SimpleFeatureCollection::getGlobalMDStringIterator() {
	return &global_md_string;
}

DirectMetadata<double>* SimpleFeatureCollection::getGlobalMDValueIterator() {
	return &global_md_value;
}

std::vector<std::string> SimpleFeatureCollection::getGlobalMDValueKeys() const {
	std::vector<std::string> keys;
	for (auto keyValue : global_md_value) {
		keys.push_back(keyValue.first);
	}
	return keys;
}

std::vector<std::string> SimpleFeatureCollection::getGlobalMDStringKeys() const {
	std::vector<std::string> keys;
	for (auto keyValue : global_md_string) {
		keys.push_back(keyValue.first);
	}
	return keys;
}

void SimpleFeatureCollection::setGlobalMDString(const std::string &key, const std::string &value) {
	global_md_string.set(key, value);
}

void SimpleFeatureCollection::setGlobalMDValue(const std::string &key, double value) {
	global_md_value.set(key, value);
}

std::string SimpleFeatureCollection::toWKT() const {
	std::ostringstream wkt;

	wkt << "GEOMETRYCOLLECTION(";

	for(size_t i = 0; i < getFeatureCount(); ++i){
		wkt << featureToWKT(i) << ",";
	}
	wkt.seekp(((long) wkt.tellp()) - 1); // delete last ,

	wkt << ")";

	return wkt.str();
}
