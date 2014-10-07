#include "raster/raster.h"
#include "raster/pointcollection.h"
#include "plot/text.h"
#include "raster/profiler.h"
#include "operators/operator.h"
#include "util/make_unique.h"

#include "util/socket.h"
#include "rserver/rserver.h"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <mutex>
#include <functional>
#include <cmath>

#include <json/json.h>


class ROperator : public GenericOperator {
	public:
		ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ROperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
		virtual std::unique_ptr<PointCollection> getPoints(const QueryRectangle &rect);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect);

		void runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type);
	private:
		friend std::unique_ptr<GenericRaster> query_raster_source(ROperator *op, int childidx, const QueryRectangle &rect);
		friend std::unique_ptr<PointCollection> query_points_source(ROperator *op, int childidx, const QueryRectangle &rect);

		std::string source;
		std::string result_type;
};

ROperator::ROperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	source = params["source"].asString();
	result_type = params["result"].asString();
}
ROperator::~ROperator() {
}
REGISTER_OPERATOR(ROperator, "r");


void ROperator::runScript(BinaryStream &stream, const QueryRectangle &rect, char requested_type) {
	Profiler::Profiler p("R_OPERATOR");

	stream.write(RSERVER_MAGIC_NUMBER);
	stream.write(requested_type);
	stream.write(source);
	stream.write((int) getRasterSourceCount());
	stream.write((int) getPointsSourceCount());
	stream.write(rect);

	char type;
	while (stream.read(&type) != 0) {
		//fprintf(stderr, "Server got command %d\n", (int) type);
		if (type > 0) {
			int childidx;
			stream.read(&childidx);
			QueryRectangle qrect(stream);
			if (type == RSERVER_TYPE_RASTER) {
				auto raster = getRasterFromSource(childidx, qrect);
				stream.write(*raster);
			}
			else if (type == RSERVER_TYPE_POINTS) {
				auto points = getPointsFromSource(childidx, qrect);
				stream.write(*points);
			}
			else {
				throw OperatorException("R: invalid data type requested by server");
			}
		}
		else {
			if (type != -requested_type)
				throw OperatorException("R: wrong data type returned by server");
			return; // the caller will read the result object from the stream
		}
	}
	throw OperatorException("R: something went horribly wrong");
}


std::unique_ptr<GenericRaster> ROperator::getRaster(const QueryRectangle &rect) {
	if (result_type != "raster")
		throw OperatorException("This R script does not return rasters");

	UnixSocket socket(rserver_socket_address);
	runScript(socket, rect, RSERVER_TYPE_RASTER);

	auto raster = GenericRaster::fromStream(socket);
	return raster;
}

std::unique_ptr<PointCollection> ROperator::getPoints(const QueryRectangle &rect) {
	if (result_type != "points")
		throw OperatorException("This R script does not return a point collection");

	throw OperatorException("TODO");
	//return runScript(rect);
}

std::unique_ptr<GenericPlot> ROperator::getPlot(const QueryRectangle &rect) {
	if (result_type != "text")
		throw OperatorException("This R script does not return a plot");

	UnixSocket socket(rserver_socket_address);
	runScript(socket, rect, RSERVER_TYPE_STRING);

	std::string result;
	((BinaryStream &) socket).read(&result);
	return std::unique_ptr<GenericPlot>(new TextPlot(result));
}



