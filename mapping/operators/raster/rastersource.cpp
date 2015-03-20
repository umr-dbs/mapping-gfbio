
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "rasterdb/rasterdb.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "util/configuration.h"

#include <memory>
#include <sstream>
#include <cmath>
#include <json/json.h>


// RasterSource Operator
class SourceOperator : public GenericOperator {
	public:
		SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~SourceOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		std::shared_ptr<RasterDB> rasterdb;
		int channel;
		bool transform;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), rasterdb(nullptr) {
	assumeSources(0);
	std::string fullpath = params.get("sourcepath", "").asString();
	std::string sourcename = params.get("sourcename", "").asString();
	if (fullpath.length() > 0 && sourcename.length() > 0)
		throw OperatorException("SourceOperator: specify sourcepath or sourcename, not both");
	if (fullpath.length() == 0 && sourcename.length() == 0)
		throw OperatorException("SourceOperator: missing sourcepath or sourcename");
	std::string filename;
	if (fullpath.length() > 0)
		filename = fullpath;
	else
		filename = Configuration::get("operators.rastersource.path", "") + sourcename + std::string(".json");

	rasterdb = RasterDB::open(filename.c_str(), RasterDB::READ_ONLY);
	channel = params.get("channel", 0).asInt();
	transform = params.get("transform", true).asBool();
}

SourceOperator::~SourceOperator() {
}

REGISTER_OPERATOR(SourceOperator, "source");


std::unique_ptr<GenericRaster> SourceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	return rasterdb->query(rect, profiler, channel, transform);
}

