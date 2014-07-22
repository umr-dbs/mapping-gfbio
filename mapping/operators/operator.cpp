
#include "raster/raster.h"

#include "operators/operator.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <sstream>

#include <json/json.h>


// The magic of type registration, see REGISTER_OPERATOR in operator.h
typedef std::unique_ptr<GenericOperator> (*OPConstructor)(int sourcecount, GenericOperator *sources[], Json::Value &params);

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
GenericOperator::GenericOperator(Type type, int sourcecount, GenericOperator *_sources[]) : type(type), sourcecount(sourcecount) {
	int i=0;
	for (;i<sourcecount;i++) {
		sources[i] = _sources[i];
		_sources[i] = nullptr; // we take ownership, so make sure that GenericOperator::fromJSON() doesn't try to free it in case of exceptions
	}
	for (;i<MAX_SOURCES;i++) {
		sources[i] = nullptr;
	}
}


GenericOperator::~GenericOperator() {
	for (int i=0;i<sourcecount;i++) {
		delete sources[i];
		sources[i] = nullptr;
	}
}


void GenericOperator::assumeSources(int n) {
	if (sourcecount != n)
		throw OperatorException("Wrong amount of sources");
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
std::unique_ptr<Histogram> GenericOperator::getHistogram(const QueryRectangle &rect) {
	throw OperatorException("getHistogram() called on an operator that doesn't return histogram");
}


// JSON constructor
std::unique_ptr<GenericOperator> GenericOperator::fromJSON(Json::Value &json) {
	// recursively create all sources
	Json::Value sourcelist = json["sources"];
	Json::Value params = json["params"];
	int sourcecount = 0;
	GenericOperator *sources[MAX_SOURCES] = {nullptr};
	try {
		if (sourcelist.isArray() && sourcelist.size() > 0) {
			sourcecount = sourcelist.size();
			for (int i=0;i<sourcecount;i++) {
				sources[i] = GenericOperator::fromJSON(sourcelist[(Json::Value::ArrayIndex) i]).release();
			}
		}

		// now check the operator name and instantiate the correct class
		std::string type = json["type"].asString();

		auto map = getRegisteredConstructorsMap();
		if (map->count(type) != 1) {
			throw OperatorException(std::string("Unknown operator type: ")+type);
		}

		auto constructor = map->at(type);
		return constructor(sourcecount, sources, params);
	}
	catch (const std::exception &e) {
		for (int i=0;i<sourcecount;i++) {
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
