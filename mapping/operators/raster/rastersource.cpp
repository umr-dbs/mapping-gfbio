
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
	protected:
		void writeSemanticParameters(std::ostringstream &stream);
	private:
		std::shared_ptr<RasterDB> rasterdb;
		std::string sourcename;
		int channel;
		bool transform;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), rasterdb(nullptr) {
	assumeSources(0);
	sourcename = params.get("sourcename", "").asString();
	if (sourcename.length() == 0)
		throw OperatorException("SourceOperator: missing sourcename");

	rasterdb = RasterDB::open(sourcename.c_str(), RasterDB::READ_ONLY);
	channel = params.get("channel", 0).asInt();
	transform = params.get("transform", true).asBool();
}

SourceOperator::~SourceOperator() {
}

void SourceOperator::writeSemanticParameters(std::ostringstream &stream) {
        stream << "\"sourcename\": \"" << sourcename << "\"";
}

REGISTER_OPERATOR(SourceOperator, "rastersource");

// obsolete, keep for backwards compatibility for a while
class SourceOperator2 : public SourceOperator {
	public:
		SourceOperator2(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : SourceOperator(sourcecounts, sources, params) {}
};
REGISTER_OPERATOR(SourceOperator2, "source");



std::unique_ptr<GenericRaster> SourceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	return rasterdb->query(rect, profiler, channel, transform);
}

void SourceOperator::writeSemanticParameters(std::ostringstream &stream) {
	stream << "\"sourcename\": \"" << sourcename << "\"";
}
