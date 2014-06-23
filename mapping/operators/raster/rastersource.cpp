
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
		SourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~SourceOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
	private:
		RasterSource *rastersource;
		int channel;
};


// RasterSource Operator
SourceOperator::SourceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources), rastersource(nullptr) {
	assumeSources(0);
	std::string filename = params.get("sourcepath", "").asString();
	rastersource = RasterSourceManager::open(filename.c_str());
	channel = params.get("channel", 0).asInt();
}

SourceOperator::~SourceOperator() {
	RasterSourceManager::close(rastersource);
	rastersource = nullptr;
}

REGISTER_OPERATOR(SourceOperator, "source");

//GenericRaster *SourceOperator::execute(int timestamp, double x1, double y1, double x2, double y2, int xres, int yres) {
GenericRaster *SourceOperator::getRaster(const QueryRectangle &rect) {
	Profiler::Profiler p("SOURCE_OPERATOR");

	const LocalCRS *rmd = rastersource->getRasterMetadata();

#if 0
	return rastersource->load(channel, timestamp, rect.x1, rect.y1, rect.x2, rect.y2);
#else
	// world to pixel coordinates
	double px1 = rmd->WorldToPixelX(rect.x1);
	double py1 = rmd->WorldToPixelY(rect.y1);
	double px2 = rmd->WorldToPixelX(rect.x2);
	double py2 = rmd->WorldToPixelY(rect.y2);
	// TODO: ist px2 inclusive, exclusive? auf- oder abrunden?
	//printf("SourceOperator: (%f, %f -> %f, %f) to (%d, %d -> %d, %d)\n", x1, y1, x2, y2, px1, py1, px2, py2);

	// TODO: skalieren auf xres/yres, nach pyramide laden, oder was?
	return rastersource->load(channel, rect.timestamp, std::floor(std::min(px1,px2)), std::floor(std::min(py1,py2)), std::ceil(std::max(px1,px2))+1, std::ceil(std::max(py1,py2))+1);
#endif
}

