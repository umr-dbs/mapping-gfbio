
#include "datatypes/raster.h"
#include "datatypes/plot.h"
#include "util/binarystream.h"
#include "util/debug.h"

#include "operators/operator.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <sstream>
#include <unistd.h>
#include <time.h>


#include <json/json.h>
#include "datatypes/linecollection.h"
#include "datatypes/pointcollection.h"
#include "datatypes/polygoncollection.h"


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


/*
 * QueryRectangle class
 */
QueryRectangle::QueryRectangle(const GridSpatioTemporalResult &grid)
	: QueryRectangle(grid.stref.t1, grid.stref.x1, grid.stref.y1, grid.stref.x2, grid.stref.y2, grid.width, grid.height, grid.stref.epsg){
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



/*
 * QueryProfiler class
 */
QueryProfiler::QueryProfiler() : self_cpu(0), all_cpu(0), self_gpu(0), all_gpu(0), self_io(0), all_io(0), t_start(0) {

}

double QueryProfiler::getTimestamp() {
#if defined(_POSIX_TIMERS) && defined(_POSIX_CPUTIME)
	struct timespec t;
	if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t) != 0)
		throw OperatorException("QueryProfiler: clock_gettime() failed");
	return (double) t.tv_sec + t.tv_nsec/1000000000.0;
#else
	#warning "QueryProfiler: Cannot query CPU time on this OS, using wall time instead"

	struct timeval t;
	if (gettimeofday(&t, nullptr) != 0)
		throw OperatorException("QueryProfiler: gettimeofday() failed");
	return (double) t.tv_sec + t.tv_usec/1000000.0;
#endif
}

void QueryProfiler::startTimer() {
	if (t_start != 0)
		throw OperatorException("QueryProfiler: Timer started twice");
	t_start = getTimestamp();
}

void QueryProfiler::stopTimer() {
	if (t_start == 0)
		throw OperatorException("QueryProfiler: Timer not started");
	double cost = getTimestamp() - t_start;
	t_start = 0;
	if (cost < 0)
		throw OperatorException("QueryProfiler: Timer stopped a negative time");
	self_cpu += cost;
	all_cpu += cost;
}

void QueryProfiler::addGPUCost(double seconds) {
	self_gpu += seconds;
	all_gpu += seconds;
}

void QueryProfiler::addIOCost(size_t bytes) {
	self_io += bytes;
	all_io += bytes;
}

QueryProfiler & QueryProfiler::operator+=(QueryProfiler &other) {
	if (other.t_start != 0)
		throw OperatorException("QueryProfiler: tried adding a timer that had not been stopped");
	all_cpu += other.all_cpu;
	all_gpu += other.all_gpu;
	all_io += other.all_io;
	return *this;
}

// these are two RAII helper classes to make sure that profiling works even when an operator throws an exception
class QueryProfilerRunningGuard {
	public:
		QueryProfilerRunningGuard(QueryProfiler &parent_profiler, QueryProfiler &profiler)
			: parent_profiler(parent_profiler), profiler(profiler) {
			profiler.startTimer();
		}
		~QueryProfilerRunningGuard() {
			profiler.stopTimer();
			parent_profiler += profiler;

		}
		QueryProfiler &parent_profiler;
		QueryProfiler &profiler;
};
class QueryProfilerStoppingGuard {
	public:
		QueryProfilerStoppingGuard(QueryProfiler &profiler) : profiler(profiler) {
			profiler.stopTimer();
		}
		~QueryProfilerStoppingGuard() {
			profiler.startTimer();
		}
		QueryProfiler &profiler;
};


/*
 * GenericOperator class
 */
GenericOperator::GenericOperator(int _sourcecounts[], GenericOperator *_sources[]) : type(), semantic_id(), depth(0) {
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

void GenericOperator::writeSemanticParameters(std::ostringstream &) {
}

void GenericOperator::assumeSources(int rasters, int pointcollections, int linecollections, int polygoncollections) {
	return;
	if (rasters >= 0 && sourcecounts[0] != rasters)
		throw OperatorException("Wrong amount of raster sources");
	if (pointcollections >= 0 && sourcecounts[1] !=pointcollections)
		throw OperatorException("Wrong amount of pointcollection sources");
	if (linecollections >= 0 && sourcecounts[2] != linecollections)
			throw OperatorException("Wrong amount of linecollection sources");
	if (polygoncollections >= 0 && sourcecounts[3] != polygoncollections)
		throw OperatorException("Wrong amount of polygoncollection sources");
}

std::unique_ptr<GenericRaster> GenericOperator::getRaster(const QueryRectangle &, QueryProfiler &) {
	throw OperatorException("getRaster() called on an operator that doesn't return rasters");
}
std::unique_ptr<PointCollection> GenericOperator::getPointCollection(const QueryRectangle &, QueryProfiler &) {
	throw OperatorException("getPointCollection() called on an operator that doesn't return points");
}
std::unique_ptr<LineCollection> GenericOperator::getLineCollection(const QueryRectangle &, QueryProfiler &) {
	throw OperatorException("getLineCollection() called on an operator that doesn't return lines");
}
std::unique_ptr<PolygonCollection> GenericOperator::getPolygonCollection(const QueryRectangle &, QueryProfiler &) {
	throw OperatorException("getPolygonCollection() called on an operator that doesn't return polygons");
}
std::unique_ptr<GenericPlot> GenericOperator::getPlot(const QueryRectangle &, QueryProfiler &) {
	throw OperatorException("getPlot() called on an operator that doesn't return data vectors");
}

static void d_profile(int depth, const std::string &type, const char *result, QueryProfiler &profiler, size_t bytes = 0) {
	std::ostringstream msg;
	msg.precision(4);
	for (int i=0;i<depth;i++)
		msg << " ";
	msg << std::fixed << "OP " << type << " " << result
		<< " CPU: " << profiler.self_cpu << "/" << profiler.all_cpu
		<< " GPU: " << profiler.self_gpu << "/" << profiler.all_gpu
		<< " I/O: " << profiler.self_io << "/" << profiler.all_io;
	if (bytes > 0) {
		// Estimate the costs to cache this item
		double cache_cpu = 0.000000005 * bytes;
		size_t cache_io = bytes;
		msg << "  Caching CPU: " << cache_cpu << " I/O: " << cache_io;
		if (2 * cache_cpu < (profiler.all_cpu + profiler.all_gpu) || 2 * cache_io < profiler.all_io)
			msg << " CACHE CANDIDATE";
	}
	d(msg.str());
}

std::unique_ptr<GenericRaster> GenericOperator::getCachedRaster(const QueryRectangle &rect, QueryProfiler &parent_profiler, RasterQM query_mode) {
	QueryProfiler profiler;
	std::unique_ptr<GenericRaster> result;
	{
		RasterProducer producer(*this,rect,profiler);
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = CacheManager::getInstance().getRaster(
				semantic_id,
				rect,
				producer
		);
	}
	d_profile(depth, type, "raster", profiler, result->getDataSize());

	// the costs of adjusting the result are assigned to the calling operator
	if (query_mode == RasterQM::EXACT)
		result = result->fitToQueryRectangle(rect);
	return result;
}
std::unique_ptr<PointCollection> GenericOperator::getCachedPointCollection(const QueryRectangle &rect, QueryProfiler &parent_profiler, FeatureCollectionQM query_mode) {
	QueryProfiler profiler;
	std::unique_ptr<PointCollection> result;
	{
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = getPointCollection(rect, profiler);
	}
	d_profile(depth, type, "points", profiler);

	if (query_mode == FeatureCollectionQM::SINGLE_ELEMENT_FEATURES && !result->isSimple())
		throw OperatorException("Operator did not return Features consisting only of single points");
	return result;
}
std::unique_ptr<LineCollection> GenericOperator::getCachedLineCollection(const QueryRectangle &rect, QueryProfiler &parent_profiler, FeatureCollectionQM query_mode) {
	QueryProfiler profiler;
	std::unique_ptr<LineCollection> result;
	{
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = getLineCollection(rect, profiler);
	}
	d_profile(depth, type, "lines", profiler);

	if (query_mode == FeatureCollectionQM::SINGLE_ELEMENT_FEATURES && !result->isSimple())
		throw OperatorException("Operator did not return Features consisting only of single lines");
	return result;
}
std::unique_ptr<PolygonCollection> GenericOperator::getCachedPolygonCollection(const QueryRectangle &rect, QueryProfiler &parent_profiler, FeatureCollectionQM query_mode) {
	QueryProfiler profiler;
	std::unique_ptr<PolygonCollection> result;
	{
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = getPolygonCollection(rect, profiler);
	}
	d_profile(depth, type, "polygons", profiler);

	if (query_mode == FeatureCollectionQM::SINGLE_ELEMENT_FEATURES && !result->isSimple())
		throw OperatorException("Operator did not return Features consisting only of single polygons");
	return result;
}
std::unique_ptr<GenericPlot> GenericOperator::getCachedPlot(const QueryRectangle &rect, QueryProfiler &parent_profiler) {
	QueryProfiler profiler;
	std::unique_ptr<GenericPlot> result;
	{
		QueryProfilerRunningGuard guard(parent_profiler, profiler);
		result = getPlot(rect, profiler);
	}
	d_profile(depth, type, "plot", profiler);
	return result;
}


std::unique_ptr<GenericRaster> GenericOperator::getRasterFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, RasterQM query_mode) {
	if (idx < 0 || idx >= sourcecounts[0])
		throw OperatorException("getChildRaster() called on invalid index");
	QueryProfilerStoppingGuard guard(profiler);
	auto result = sources[idx]->getCachedRaster(rect, profiler, query_mode);
	return result;
}
std::unique_ptr<PointCollection> GenericOperator::getPointCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode) {
	if (idx < 0 || idx >= sourcecounts[1])
		throw OperatorException("getChildPoints() called on invalid index");
	QueryProfilerStoppingGuard guard(profiler);
	int offset = sourcecounts[0] + idx;
	auto result = sources[offset]->getCachedPointCollection(rect, profiler, query_mode);
	return result;
}
std::unique_ptr<LineCollection> GenericOperator::getLineCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode) {
	if (idx < 0 || idx >= sourcecounts[2])
		throw OperatorException("getChildLines() called on invalid index");
	QueryProfilerStoppingGuard guard(profiler);
	int offset = sourcecounts[0] + sourcecounts[1] + idx;
	auto result = sources[offset]->getCachedLineCollection(rect, profiler, query_mode);
	return result;
}
std::unique_ptr<PolygonCollection> GenericOperator::getPolygonCollectionFromSource(int idx, const QueryRectangle &rect, QueryProfiler &profiler, FeatureCollectionQM query_mode) {
	if (idx < 0 || idx >= sourcecounts[3]){
		std::stringstream sstm;
		sstm << "getChildPolygons() called on invalid index: " << idx;
		throw OperatorException(sstm.str());
	}
	QueryProfilerStoppingGuard guard(profiler);
	int offset = sourcecounts[0] + sourcecounts[1] + sourcecounts[2] + idx;
	auto result = sources[offset]->getCachedPolygonCollection(rect, profiler, query_mode);
	return result;
}


// JSON constructor
static int parseSourcesFromJSON(Json::Value &sourcelist, GenericOperator *sources[GenericOperator::MAX_SOURCES], int &sourcecount, int depth) {
	if (!sourcelist.isArray() || sourcelist.size() <= 0)
		return 0;

	int newsources = sourcelist.size();

	if (sourcecount + newsources >= GenericOperator::MAX_SOURCES)
		throw OperatorException("Operator with more than MAX_SOURCES found; increase the constant and recompile");

	for (int i=0;i<newsources;i++) {
		sources[sourcecount+i] = GenericOperator::fromJSON(sourcelist[(Json::Value::ArrayIndex) i], depth).release();
	}

	sourcecount += newsources;
	return newsources;
}

std::unique_ptr<GenericOperator> GenericOperator::fromJSON(Json::Value &json, int depth) {
	std::string sourcetypes[] = { "raster", "points", "lines", "polygons" };

	// recursively create all sources
	Json::Value sourcelist = json["sources"];
	Json::Value params = json["params"];
	if (!params.isObject()) {
		params = Json::objectValue;
	}

	int sourcecount = 0;
	int sourcecounts[MAX_INPUT_TYPES] = {0};
	GenericOperator *sources[MAX_SOURCES] = {nullptr};
	try {
		// Instantiate all the sources
		if (sourcelist.isObject()) {
			for (int i=0;i<MAX_INPUT_TYPES;i++)
				sourcecounts[i] = parseSourcesFromJSON(sourcelist[sourcetypes[i]], sources, sourcecount, depth+1);
		}

		// now check the operator name and instantiate the correct class
		std::string type = json["type"].asString();

		auto map = getRegisteredConstructorsMap();
		if (map->count(type) != 1)
			throw OperatorException(std::string("Unknown operator type: '")+type+"'");

		auto constructor = map->at(type);
		auto op = constructor(sourcecounts, sources, params);
		op->type = type;
		op->depth = depth;

		// Finally construct the semantic id
		std::ostringstream semantic_id;
		semantic_id << "{ \"type\": \"" << type << "\", \"params\": {";
		op->writeSemanticParameters(semantic_id);
		semantic_id << "}, \"sources\":{";
		int sourceidx = 0;
		bool first_sourcetype = true;
		for (int i=0;i<MAX_INPUT_TYPES;i++) {
			int sourcecount = op->sourcecounts[i];
			if (sourcecount > 0) {
				if (!first_sourcetype)
					semantic_id << ",";
				first_sourcetype = false;
				semantic_id << "\"" << sourcetypes[i] << "\": [";
				for (int j=0;j<sourcecount;j++) {
					if (j > 0)
						semantic_id << ",";
					semantic_id << op->sources[sourceidx++]->semantic_id;
				}
				semantic_id << "]";
			}
		}
		semantic_id << "}}";
		op->semantic_id = semantic_id.str();
		return op;
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


std::unique_ptr<GenericOperator> GenericOperator::fromJSON(const std::string &json, int depth) {
	std::istringstream iss(json);
	Json::Reader reader(Json::Features::strictMode());
	Json::Value root;
	if (!reader.parse(iss, root))
		throw OperatorException("unable to parse json");

	return GenericOperator::fromJSON(root, depth);
}
