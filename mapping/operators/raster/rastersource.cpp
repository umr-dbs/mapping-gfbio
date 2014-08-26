
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/rastersource.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <json/json.h>


// RasterSource Operator
class SourceOperator : public GenericOperator {
	public:
		SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~SourceOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
		RasterSource *rastersource;
		int channel;
		bool transform;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), rastersource(nullptr) {
	assumeSources(0);
	std::string filename = params.get("sourcepath", "").asString();
	rastersource = RasterSourceManager::open(filename.c_str());
	channel = params.get("channel", 0).asInt();
	transform = params.get("transform", true).asBool();
}

SourceOperator::~SourceOperator() {
	RasterSourceManager::close(rastersource);
	rastersource = nullptr;
}

REGISTER_OPERATOR(SourceOperator, "source");


std::unique_ptr<GenericRaster> SourceOperator::getRaster(const QueryRectangle &rect) {
	Profiler::Profiler p("SOURCE_OPERATOR");

	const LocalCRS *lcrs = rastersource->getLocalCRS();

	if (lcrs->epsg != rect.epsg)
		throw OperatorException("SourceOperator: wrong epsg requested");

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
	int pixel_width = pixel_x2 - pixel_x1;
	int pixel_height = pixel_y2 - pixel_y1;
	while (pixel_width > 2*rect.xres && pixel_height > 2*rect.yres) {
		zoom++;
		pixel_width >>= 1;
		pixel_height >>= 1;
	}

	return rastersource->load(channel, rect.timestamp, pixel_x1, pixel_y1, pixel_x2, pixel_y2, zoom, transform);
}

