
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/rastersource.h"
#include "raster/opencl.h"
#include "operators/operator.h"

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
		RasterSource *rastersource;
		int channel;
		bool transform;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), rastersource(nullptr) {
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
		filename = std::string("datasources/") + sourcename + std::string(".json");

	rastersource = RasterSourceManager::open(filename.c_str());
	channel = params.get("channel", 0).asInt();
	transform = params.get("transform", true).asBool();
}

SourceOperator::~SourceOperator() {
	RasterSourceManager::close(rastersource);
	rastersource = nullptr;
}

REGISTER_OPERATOR(SourceOperator, "source");


std::unique_ptr<GenericRaster> SourceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	const LocalCRS *lcrs = rastersource->getLocalCRS();

	if (lcrs->epsg != rect.epsg) {
		std::stringstream msg;
		msg << "SourceOperator: wrong epsg requested. Source is " << (int) lcrs->epsg << ", requested " << (int) rect.epsg;
		throw OperatorException(msg.str());
	}

	// world to pixel coordinates
	double px1 = lcrs->WorldToPixelX(rect.x1);
	double py1 = lcrs->WorldToPixelY(rect.y1);
	double px2 = lcrs->WorldToPixelX(rect.x2);
	double py2 = lcrs->WorldToPixelY(rect.y2);
	// TODO: ist px2 inclusive, exclusive? auf- oder abrunden?
	//printf("SourceOperator: (%f, %f -> %f, %f) to (%d, %d -> %d, %d)\n", x1, y1, x2, y2, px1, py1, px2, py2);


	// Figure out the desired zoom level
	int pixel_x1 = std::floor(std::min(px1,px2));
	int pixel_y1 = std::floor(std::min(py1,py2));
	int pixel_x2 = std::ceil(std::max(px1,px2))+1;
	int pixel_y2 = std::ceil(std::max(py1,py2))+1;

	int zoom = 0;
	uint32_t pixel_width = pixel_x2 - pixel_x1;
	uint32_t pixel_height = pixel_y2 - pixel_y1;
	while (pixel_width > 2*rect.xres && pixel_height > 2*rect.yres) {
		zoom++;
		pixel_width >>= 1;
		pixel_height >>= 1;
	}

	size_t io_costs = 0;
	auto result = rastersource->load(channel, rect.timestamp, pixel_x1, pixel_y1, pixel_x2, pixel_y2, zoom, transform, &io_costs);
	profiler.addIOCost(io_costs);
	return result;
}

