
#include "simplefeaturecollection.h"
#include "raster/exceptions.h"
#include "util/binarystream.h"
#include "util/hash.h"
#include "util/make_unique.h"

#include <sstream>
#include <iomanip>
#include <iostream>
#include <cmath>


Point::Point(double x, double y) : x(x), y(y) {
}

Point::~Point() {
}

Point::Point(BinaryStream &stream) {
	stream.read(&x);
	stream.read(&y);
}
void Point::toStream(BinaryStream &stream) {
	stream.write(x);
	stream.write(y);
}


SimpleFeatureCollection::SimpleFeatureCollection(const SpatioTemporalReference &stref) : SpatioTemporalResult(stref), has_time(false) {

}

SimpleFeatureCollection::~SimpleFeatureCollection() {

}


//Metadata
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
