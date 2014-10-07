
#include "raster/raster.h"
#include "raster/pointcollection.h"
#include "raster/geometry.h"
#include "plot/plot.h"
#include "util/binarystream.h"

#include "operators/operator.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <sstream>

#include <json/json.h>


// The magic of type registration, see REGISTER_OPERATOR in operator.h
typedef std::unique_ptr<GenericOperator> (*OPConstructor)(int sourcecounts[], GenericOperator *sources[], Json::Value &params);

static std::unordered_map< std::string, OPConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, OPConstructor > registered_constructors;
	return &registered_constructors;
}

OperatorRegistration::OperatorRegistration(const char *name, OPConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
/*
	printf("Adding constructor for '%s'\n", name);
	for (auto i : *map) {
		printf(" contains %s\n", i.first.c_str());
	}
*/
}



QueryRectangle::QueryRectangle(BinaryStream &socket) {
	socket.read(&timestamp);
	socket.read(&x1);
	socket.read(&y1);
	socket.read(&x2);
	socket.read(&y2);
	socket.read(&xres);
	socket.read(&yres);
	socket.read(&epsg);
}

void QueryRectangle::toStream(BinaryStream &stream) const {
	stream.write(timestamp);
	stream.write(x1);
	stream.write(y1);
	stream.write(x2);
	stream.write(y2);
	stream.write(xres);
	stream.write(yres);
	stream.write(epsg);
}



double QueryRectangle::minx() const { return std::min(x1, x2); }
double QueryRectangle::maxx() const { return std::max(x1, x2); }
double QueryRectangle::miny() const { return std::min(y1, y2); }
double QueryRectangle::maxy() const { return std::max(y1, y2); }


void QueryRectangle::enlarge(int pixels) {
	double pixel_size_in_world_coordinates_x = (double) std::abs(x2 - x1) / xres;
	double pixel_size_in_world_coordinates_y = (double) std::abs(y2 - y1) / yres;

	x1 -= pixels * pixel_size_in_world_coordinates_x;
	x2 += pixels * pixel_size_in_world_coordinates_x;
	y1 -= pixels * pixel_size_in_world_coordinates_y;
	y2 += pixels * pixel_size_in_world_coordinates_y;

	xres += 2*pixels;
	yres += 2*pixels;
}



// GenericOperator default implementation
GenericOperator::GenericOperator(int _sourcecounts[], GenericOperator *_sources[]) {
	for (int i=0;i<MAX_INPUT_TYPES;i++)
		sourcecounts[i] = _sourcecounts[i];

	for (int i=0;i<MAX_SOURCES;i++) {
		sources[i] = _sources[i];
		// we take ownership, so make sure that GenericOperator::fromJSON() doesn't try to free it in case of exceptions
		_sources[i] = nullptr;
	}
}


GenericOperator::~GenericOperator() {
	for (int i=0;i<MAX_INPUT_TYPES;i++) {
		delete sources[i];
		sources[i] = nullptr;
	}
}


void GenericOperator::assumeSources(int rasters, int pointcollections, int geometries) {
	return;
	if (rasters >= 0 && sourcecounts[0] != rasters)
		throw OperatorException("Wrong amount of raster sources");
	if (pointcollections >= 0 && sourcecounts[1] != pointcollections)
		throw OperatorException("Wrong amount of pointcollection sources");
	if (geometries >= 0 && sourcecounts[2] != geometries)
		throw OperatorException("Wrong amount of geometry sources");
}

std::unique_ptr<GenericRaster> GenericOperator::getRaster(const QueryRectangle &) {
	throw OperatorException("getRaster() called on an operator that doesn't return rasters");
}
std::unique_ptr<PointCollection> GenericOperator::getPoints(const QueryRectangle &) {
	throw OperatorException("getPoints() called on an operator that doesn't return points");
}
std::unique_ptr<GenericGeometry> GenericOperator::getGeometry(const QueryRectangle &) {
	throw OperatorException("getGeometry() called on an operator that doesn't return geometries");
}
std::unique_ptr<GenericPlot> GenericOperator::getPlot(const QueryRectangle &) {
	throw OperatorException("getPlot() called on an operator that doesn't return data vectors");
}

std::unique_ptr<GenericRaster> GenericOperator::getCachedRaster(const QueryRectangle &rect) {
	return getRaster(rect);
}
std::unique_ptr<PointCollection> GenericOperator::getCachedPoints(const QueryRectangle &rect) {
	return getPoints(rect);
}
std::unique_ptr<GenericGeometry> GenericOperator::getCachedGeometry(const QueryRectangle &rect) {
	return getGeometry(rect);
}
std::unique_ptr<GenericPlot> GenericOperator::getCachedPlot(const QueryRectangle &rect) {
	return getPlot(rect);
}


std::unique_ptr<GenericRaster> GenericOperator::getRasterFromSource(int idx, const QueryRectangle &rect) {
	if (idx < 0 || idx >= sourcecounts[0])
		throw OperatorException("getChildRaster() called on invalid index");
	return std::move(sources[idx]->getCachedRaster(rect));
}
std::unique_ptr<PointCollection> GenericOperator::getPointsFromSource(int idx, const QueryRectangle &rect) {
	if (idx < 0 || idx >= sourcecounts[1])
		throw OperatorException("getChildPoints() called on invalid index");
	int offset = sourcecounts[0] + idx;
	return std::move(sources[offset]->getCachedPoints(rect));
}
std::unique_ptr<GenericGeometry> GenericOperator::getGeometryFromSource(int idx, const QueryRectangle &rect) {
	if (idx < 0 || idx >= sourcecounts[2])
		throw OperatorException("getChildGeometry() called on invalid index");
	int offset = sourcecounts[0] + sourcecounts[1] + idx;
	return std::move(sources[offset]->getCachedGeometry(rect));
}


// JSON constructor
static int parseSourcesFromJSON(Json::Value &sourcelist, GenericOperator *sources[GenericOperator::MAX_SOURCES], int &sourcecount) {
	if (!sourcelist.isArray() || sourcelist.size() <= 0)
		return sourcecount;

	int newsources = sourcelist.size();

	if (sourcecount + newsources >= GenericOperator::MAX_SOURCES)
		throw OperatorException("Operator with more than MAX_SOURCES found; increase the constant and recompile");

	for (int i=0;i<newsources;i++) {
		sources[sourcecount+i] = GenericOperator::fromJSON(sourcelist[(Json::Value::ArrayIndex) i]).release();
	}

	sourcecount += newsources;
	return newsources;
}

std::unique_ptr<GenericOperator> GenericOperator::fromJSON(Json::Value &json) {
	// recursively create all sources
	Json::Value sourcelist = json["sources"];
	Json::Value params = json["params"];
	int sourcecount = 0;
	int sourcecounts[MAX_INPUT_TYPES] = {0};
	GenericOperator *sources[MAX_SOURCES] = {nullptr};
	try {
		sourcecounts[0] = parseSourcesFromJSON(sourcelist["raster"], sources, sourcecount);
		sourcecounts[1] = parseSourcesFromJSON(sourcelist["points"], sources, sourcecount);
		sourcecounts[2] = parseSourcesFromJSON(sourcelist["geometry"], sources, sourcecount);

		// now check the operator name and instantiate the correct class
		std::string type = json["type"].asString();

		auto map = getRegisteredConstructorsMap();
		if (map->count(type) != 1) {
			throw OperatorException(std::string("Unknown operator type: ")+type);
		}

		auto constructor = map->at(type);
		return constructor(sourcecounts, sources, params);
	}
	catch (const std::exception &e) {
		for (int i=0;i<MAX_SOURCES;i++) {
			if (sources[i]) {
				delete sources[i];
				sources[i] = nullptr;
			}
		}
		throw;
	}
}


std::unique_ptr<GenericOperator> GenericOperator::fromJSON(const std::string &json) {
	std::istringstream iss(json);
	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(iss, root))
		throw OperatorException("unable to parse json");

	return GenericOperator::fromJSON(root);
}
