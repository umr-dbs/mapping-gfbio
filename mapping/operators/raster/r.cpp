#include "datatypes/raster.h"
#include "datatypes/plots/text.h"
#include "datatypes/plots/png.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include "util/binarystream.h"
#include "util/configuration.h"
#include "rserver/rserver.h"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <mutex>
#include <functional>
#include <cmath>

#include <json/json.h>
#include "datatypes/pointcollection.h"


class ROperator : public GenericOperator {
	public:
		ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ROperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);

		void runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type, QueryProfiler &profiler);
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		friend std::unique_ptr<GenericRaster> query_raster_source(ROperator *op, int childidx, const QueryRectangle &rect);
		friend std::unique_ptr<PointCollection> query_points_source(ROperator *op, int childidx, const QueryRectangle &rect);

		std::string source;
		std::string result_type;
		std::string socketpath;
};


static void replace_all(std::string &str, const std::string &search, const std::string &replace) {
    size_t start_pos = 0;
    while ((start_pos = str.find(search, start_pos)) != std::string::npos) {
        str.replace(start_pos, search.length(), replace);
        start_pos += replace.length();
    }
}

ROperator::ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	source = params["source"].asString();
	replace_all(source, "\r\n", "\n");
	result_type = params["result"].asString();
	socketpath = Configuration::get("operators.r.socket");
}
ROperator::~ROperator() {
}
REGISTER_OPERATOR(ROperator, "r");

void ROperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"source\":\"" << source << "\","
			<< "\"result_type\":\"" << result_type << "\"";
}

void ROperator::runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type, QueryProfiler &profiler) {
	stream.write(RSERVER_MAGIC_NUMBER);
	stream.write(requested_type);
	stream.write(source);
	stream.write((int) getRasterSourceCount());
	stream.write((int) getPointCollectionSourceCount());
	stream.write(rect);

	char type;
	while (stream.read(&type) != 0) {
		//fprintf(stderr, "Server got command %d\n", (int) type);
		if (type > 0) {
			int childidx;
			stream.read(&childidx);
			QueryRectangle qrect(stream);
			if (type == RSERVER_TYPE_RASTER) {
				auto raster = getRasterFromSource(childidx, qrect, profiler);
				stream.write(*raster);
			}
			else if (type == RSERVER_TYPE_POINTS) {
				auto points = getPointCollectionFromSource(childidx, qrect, profiler);
				stream.write(*points);
			}
			else {
				throw OperatorException("R: invalid data type requested by server");
			}
		}
		else {
			if (type == -RSERVER_TYPE_ERROR) {
				std::string err;
				stream.read(&err);
				std::stringstream msg;
				msg << "R exception: " << err;
				throw OperatorException(msg.str());
			}
			if (type != -requested_type)
				throw OperatorException("R: wrong data type returned by server");
			return; // the caller will read the result object from the stream
		}
	}
	throw OperatorException("R: something went horribly wrong");
}


std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (result_type != "raster")
		throw OperatorException("This R script does not return rasters");

	UnixSocket socket(socketpath.c_str());
	runScript(socket, rect, RSERVER_TYPE_RASTER, profiler);

	auto raster = GenericRaster::fromStream(socket);
	return raster;
}

std::unique_ptr<PointCollection> ROperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (result_type != "points")
		throw OperatorException("This R script does not return a point collection");

	UnixSocket socket(socketpath.c_str());
	runScript(socket, rect, RSERVER_TYPE_POINTS, profiler);

	auto points = make_unique<PointCollection>(socket);
	return points;
}

std::unique_ptr<GenericPlot> ROperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	bool wants_text = (result_type == "text");
	bool wants_plot = (result_type == "plot");
	if (!wants_text && !wants_plot)
		throw OperatorException("This R script does not return a plot");

	UnixSocket socket(socketpath.c_str());
	runScript(socket, rect, wants_text ? RSERVER_TYPE_STRING : RSERVER_TYPE_PLOT, profiler);

	std::string result;
	((BinaryStream &) socket).read(&result);
	if (wants_text)
		return std::unique_ptr<GenericPlot>(new TextPlot(result));
	else
		return std::unique_ptr<GenericPlot>(new PNGPlot(result));
}



