
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

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream &stream);
	private:
#ifndef MAPPING_OPERATOR_STUBS
		std::shared_ptr<RasterDB> rasterdb;
#endif
		std::string sourcename;
		int channel;
		bool transform;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(0);
	sourcename = params.get("sourcename", "").asString();
	if (sourcename.length() == 0)
		throw OperatorException("SourceOperator: missing sourcename");

#ifndef MAPPING_OPERATOR_STUBS
	rasterdb = RasterDB::open(sourcename.c_str(), RasterDB::READ_ONLY);
#endif
	channel = params.get("channel", 0).asInt();
	transform = params.get("transform", true).asBool();
}

SourceOperator::~SourceOperator() {
}

REGISTER_OPERATOR(SourceOperator, "rastersource");

// obsolete, keep for backwards compatibility for a while
class SourceOperator2 : public SourceOperator {
	public:
		SourceOperator2(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : SourceOperator(sourcecounts, sources, params) {}
};
REGISTER_OPERATOR(SourceOperator2, "source");


#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<GenericRaster> SourceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	return rasterdb->query(rect, profiler, channel, transform);
}
#endif

void SourceOperator::writeSemanticParameters(std::ostringstream &stream) {
	std::string trans = transform ? "true" : "false";
	stream << "{\"sourcename\": \"" << sourcename << "\",";
	stream << " \"channel\": " << channel << ",";
	stream << " \"transform\": " << trans << "}";
}
