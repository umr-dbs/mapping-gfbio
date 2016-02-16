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

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<PointCollection> getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);

		std::unique_ptr<BinaryReadBuffer> runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type, QueryProfiler &profiler);
#endif
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
	stream << "{\"source\":\"" << source << "\","
			<< "\"result_type\":\"" << result_type << "\"}";
}

#ifndef MAPPING_OPERATOR_STUBS
std::unique_ptr<BinaryReadBuffer> ROperator::runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type, QueryProfiler &profiler) {

	BinaryWriteBuffer request;
	request.write(RSERVER_MAGIC_NUMBER);
	request.write(requested_type);
	request.write(source);
	request.write((int) getRasterSourceCount());
	request.write((int) getPointCollectionSourceCount());
	request.write(rect);
	stream.write(request);

	char type;
	while (true) {
		auto response = make_unique<BinaryReadBuffer>();
		stream.read(*response);
		auto type = response->read<char>();
		//fprintf(stderr, "Server got command %d\n", (int) type);
		if (type > 0) {
			auto childidx = response->read<int>();
			QueryRectangle qrect(*response);
			if (type == RSERVER_TYPE_RASTER) {
				auto raster = getRasterFromSource(childidx, qrect, profiler);
				BinaryWriteBuffer requested_data;
				requested_data.enableLinking();
				requested_data.write(*raster);
				stream.write(requested_data);
			}
			else if (type == RSERVER_TYPE_POINTS) {
				auto points = getPointCollectionFromSource(childidx, qrect, profiler);
				BinaryWriteBuffer requested_data;
				requested_data.enableLinking();
				requested_data.write(*points);
				stream.write(requested_data);
			}
			else {
				throw OperatorException("R: invalid data type requested by server");
			}
		}
		else {
			if (type == -RSERVER_TYPE_ERROR) {
				std::string err;
				response->read(&err);
				throw OperatorException(concat("R exception: ", err));
			}
			if (type != -requested_type)
				throw OperatorException("R: wrong data type returned by server");
			return response; // the caller will read the result object from the stream
		}
	}
	throw OperatorException("R: something went horribly wrong");
}


std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (result_type != "raster")
		throw OperatorException("This R script does not return rasters");

	BinaryFDStream socket(socketpath.c_str());
	auto response = runScript(socket, rect, RSERVER_TYPE_RASTER, profiler);
	socket.close();

	auto raster = GenericRaster::fromStream(*(response.get()));
	return raster;
}

std::unique_ptr<PointCollection> ROperator::getPointCollection(const QueryRectangle &rect, QueryProfiler &profiler) {
	if (result_type != "points")
		throw OperatorException("This R script does not return a point collection");

	BinaryFDStream socket(socketpath.c_str());
	auto response = runScript(socket, rect, RSERVER_TYPE_POINTS, profiler);
	socket.close();

	auto points = make_unique<PointCollection>(*(response.get()));
	return points;
}

std::unique_ptr<GenericPlot> ROperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	bool wants_text = (result_type == "text");
	bool wants_plot = (result_type == "plot");
	if (!wants_text && !wants_plot)
		throw OperatorException("This R script does not return a plot");

	BinaryFDStream socket(socketpath.c_str());
	auto response = runScript(socket, rect, wants_text ? RSERVER_TYPE_STRING : RSERVER_TYPE_PLOT, profiler);
	socket.close();

	std::string result;
	response->read(&result);
	if (wants_text)
		return std::unique_ptr<GenericPlot>(new TextPlot(result));
	else
		return std::unique_ptr<GenericPlot>(new PNGPlot(result));
}

#endif

