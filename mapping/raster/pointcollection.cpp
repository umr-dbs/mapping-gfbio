#include "raster/pointcollection.h"


Point::Point(double x, double y, uint16_t size_md_string, uint16_t size_md_value) : x(x), y(y), md_string(size_md_string), md_value(size_md_value) {
}

Point::~Point() {
}

#if 0 // Default implementations should be equal
// Copy constructors
Point::Point(const Point &p) : x(p.x), y(p.y), md_string(p.md_string), md_value(p.md_value) {
}
Point &Point::operator=(const Point &p) {
	x = p.x;
	y = p.y;

	md_string = p.md_string;
	md_value = p.md_value;

	return *this;
}

// Move constructors
Point::Point(Point &&p) : x(p.x), y(p.y), md_string( std::move(p.md_string) ), md_value( std::move(p.md_value) ) {
}
Point &Point::operator=(Point &&p) {
	x = p.x;
	y = p.y;

	md_string = std::move(p.md_string);
	md_value = std::move(p.md_value);

	return *this;
}
#endif


/*
Point PointCollection::makePoint(double x, double y) {
	return Point(x, y, local_string_md.size(), local_double_md.size());
}
*/

PointCollection::PointCollection(epsg_t epsg) : epsg(epsg) {

}
PointCollection::~PointCollection() {

}

Point &PointCollection::addPoint(double x, double y) {
	local_md_string.lock();
	local_md_value.lock();
	//collection.emplace_back(x, y, local_md_string.size(), local_md_value.size());
	collection.push_back(Point(x, y, local_md_string.size(), local_md_value.size()));
	return collection[ collection.size() - 1 ];
}


/**
 * Global Metadata
 */
const std::string &PointCollection::getGlobalMDString(const std::string &key) {
	return global_md_string.get(key);
}

double PointCollection::getGlobalMDValue(const std::string &key) {
	return global_md_value.get(key);
}


/**
 * Local meta-metadata
 */
void PointCollection::addLocalMDString(const std::string &key) {
	local_md_string.addKey(key);
}

void PointCollection::addLocalMDValue(const std::string &key) {
	local_md_value.addKey(key);
}


/**
 * Local Metadata on points
 */
const std::string &PointCollection::getLocalMDString(const Point &point, const std::string &key) {
	return local_md_string.getValue(point.md_string, key);
}

double PointCollection::getLocalMDValue(const Point &point, const std::string &key) {
	return local_md_value.getValue(point.md_value, key);
}

void PointCollection::setLocalMDString(Point &point, const std::string &key, const std::string &value) {
	local_md_string.setValue(point.md_string, key, value);
}

void PointCollection::setLocalMDValue(Point &point, const std::string &key, double value) {
	local_md_value.setValue(point.md_value, key, value);
}

