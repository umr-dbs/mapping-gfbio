
#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "datatypes/raster/typejuggling.h"
#include "operators/operator.h"
#include "raster/opencl.h"


#include <json/json.h>
#include <memory>
#include <cmath>
#include <algorithm>
#include "datatypes/pointcollection.h"

class PointsToRasterOperator : public GenericOperator {
	public:
		PointsToRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~PointsToRasterOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		std::string renderattribute;
		double radius;
};




PointsToRasterOperator::PointsToRasterOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
	renderattribute = params.get("attribute", "").asString();
	radius = params.get("radius", 8).asDouble();
}

PointsToRasterOperator::~PointsToRasterOperator() {
}
REGISTER_OPERATOR(PointsToRasterOperator, "points2raster");

void PointsToRasterOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"renderattribute\":\"" << renderattribute << "\","
			<< "\"radius\":" << radius;
}

#include "operators/combined/points2raster_frequency.cl.h"
#include "operators/combined/points2raster_value.cl.h"


std::unique_ptr<GenericRaster> PointsToRasterOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {

	RasterOpenCL::init();

	QueryRectangle rect2 = rect;
	rect2.enlarge(radius);
	auto points = getPointCollectionFromSource(0, rect2, profiler);
	//TODO: ensure that points are single points?!

	if (renderattribute == "") {
		DataDescription dd_acc(GDT_UInt16, 0, 65535, true, 0);
		auto accumulator = GenericRaster::create(dd_acc, rect2, rect2.xres, rect2.yres, 0, GenericRaster::Representation::CPU);
		Raster2D<uint16_t> *acc = (Raster2D<uint16_t> *) accumulator.get();
		acc->clear(0);

		for (Coordinate &p : points->coordinates) {
			double x = p.x, y = p.y;

			auto px = acc->WorldToPixelX(x);
			auto py = acc->WorldToPixelY(y);
			if (px < 0 || py < 0 || px >= acc->width || py >= acc->height)
				continue;

			uint32_t val = acc->get(px, py);
			val = std::min((uint32_t) acc->dd.max, val+1);
			acc->set(px, py, val);
		}

		DataDescription dd_blur(GDT_Byte, 0, 255, true, 0);
		auto blurred = GenericRaster::create(dd_blur, rect, rect.xres, rect.yres, 0, GenericRaster::Representation::OPENCL);

		RasterOpenCL::CLProgram prog;
		prog.setProfiler(profiler);
		prog.addInRaster(accumulator.get());
		prog.addOutRaster(blurred.get());
		prog.compile(operators_combined_points2raster_frequency, "blur_frequency");
		prog.addArg(radius);
		prog.run();

		return blurred;
	}
	else {
		const float MIN = 0, MAX = 10000;
		DataDescription dd_sum(GDT_Float32, MIN, MAX, true, 0);
		DataDescription dd_count(GDT_UInt16, 0, 65534, true, 0);
		auto r_sum = GenericRaster::create(dd_sum, rect2, rect2.xres, rect2.yres, 0, GenericRaster::Representation::CPU);
		auto r_count = GenericRaster::create(dd_count, rect2, rect2.xres, rect2.yres, 0, GenericRaster::Representation::CPU);
		Raster2D<float> *sum = (Raster2D<float> *) r_sum.get();
		sum->clear(0);
		Raster2D<uint16_t> *count = (Raster2D<uint16_t> *) r_count.get();
		count->clear(0);

		auto &vec = points->local_md_value.getVector(renderattribute);
		int i=0;
		for (Coordinate &p : points->coordinates) {
			double x = p.x, y = p.y;

			auto px = sum->WorldToPixelX(x);
			auto py = sum->WorldToPixelY(y);
			if (px < 0 || py < 0 || px >= sum->width || py >= sum->height)
				continue;

			auto attr = vec[i];
			if (std::isnan(attr))
				continue;

			auto sval = sum->get(px, py);
			sval = sval+attr;
			sum->set(px, py, sval);

			auto cval = count->get(px, py);
			cval = std::min((decltype(cval+1)) count->dd.max, cval+1);
			count->set(px, py, cval);

			i++;
		}

		DataDescription dd_blur(GDT_Float32, MIN, MAX, true, 0);
		auto blurred = GenericRaster::create(dd_blur, rect, rect.xres, rect.yres, 0, GenericRaster::Representation::OPENCL);

		RasterOpenCL::CLProgram prog;
		prog.setProfiler(profiler);
		prog.addInRaster(r_count.get());
		prog.addInRaster(r_sum.get());
		prog.addOutRaster(blurred.get());
		prog.compile(operators_combined_points2raster_value, "blur_value");
		prog.addArg(radius);
		prog.run();

		return blurred;
	}
}
