
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

/**
 * Operator that rasterizes features.
 * It currently only suppors rendering a point collection as a heatmap
 *
 * Parameters:
 * - renderattribute: the name of the attribute whose frequency is counted for the heatmap
 *                    if no renderattribute is given, the location alone is taken for rendering
 * - radius: the radius for each point in the heatmap
 */
class RasterizationOperator : public GenericOperator {
	public:
		RasterizationOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~RasterizationOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		std::string renderattribute;
		double radius;
};




RasterizationOperator::RasterizationOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
	renderattribute = params.get("attribute", "").asString();
	radius = params.get("radius", 8).asDouble();
}

RasterizationOperator::~RasterizationOperator() {
}
REGISTER_OPERATOR(RasterizationOperator, "rasterization");

void RasterizationOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{\"renderattribute\":\"" << renderattribute << "\","
			<< "\"radius\":" << radius << "}";
}


#ifndef MAPPING_OPERATOR_STUBS
#ifdef MAPPING_NO_OPENCL
std::unique_ptr<GenericRaster> RasterizationOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	throw OperatorException("PointsToRasterOperator: cannot be executed without OpenCL support");
}
#else

#include "operators/processing/combined/points2raster_frequency.cl.h"
#include "operators/processing/combined/points2raster_value.cl.h"


std::unique_ptr<GenericRaster> RasterizationOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {

	RasterOpenCL::init();

	QueryRectangle rect_larger = rect;
	rect_larger.enlargePixels(radius);

	QueryRectangle rect_points(rect_larger, rect_larger, QueryResolution::none());
	auto points = getPointCollectionFromSource(0, rect_points, profiler);

	if (renderattribute == "") {
		Unit unit_acc = Unit::unknown();
		unit_acc.setMinMax(0, 65535);
		DataDescription dd_acc(GDT_UInt16, unit_acc, true, 0);
		auto accumulator = GenericRaster::create(dd_acc, rect_larger, rect_larger.xres, rect_larger.yres, 0, GenericRaster::Representation::CPU);
		Raster2D<uint16_t> *acc = (Raster2D<uint16_t> *) accumulator.get();
		acc->clear(0);
		uint16_t acc_max = std::numeric_limits<uint16_t>::max()-1;

		for (auto feature : *points) {
			for (auto &p : feature) {
				auto px = acc->WorldToPixelX(p.x);
				auto py = acc->WorldToPixelY(p.y);
				if (px < 0 || py < 0 || px >= acc->width || py >= acc->height)
					continue;

				uint32_t val = acc->get(px, py);
				val = std::min((uint32_t) acc_max, val+1);
				acc->set(px, py, val);
			}
		}

		Unit unit_blur = Unit("frequency", "heatmap");
		unit_blur.setMinMax(0, 255);
		unit_blur.setInterpolation(Unit::Interpolation::Continuous);
		DataDescription dd_blur(GDT_Byte, unit_blur, true, 0);
		auto blurred = GenericRaster::create(dd_blur, rect, rect.xres, rect.yres, 0, GenericRaster::Representation::OPENCL);

		RasterOpenCL::CLProgram prog;
		prog.setProfiler(profiler);
		prog.addInRaster(accumulator.get());
		prog.addOutRaster(blurred.get());
		prog.compile(operators_processing_combined_points2raster_frequency, "blur_frequency");
		prog.addArg(radius);
		prog.run();

		return blurred;
	}
	else {
		const float MIN = 0, MAX = 10000;
		Unit unit_sum = Unit::unknown();
		unit_sum.setMinMax(MIN, MAX);
		DataDescription dd_sum(GDT_Float32, unit_sum, true, 0);
		DataDescription dd_count(GDT_UInt16, Unit::unknown(), true, 0);
		auto r_sum = GenericRaster::create(dd_sum, rect_larger, rect_larger.xres, rect_larger.yres, 0, GenericRaster::Representation::CPU);
		auto r_count = GenericRaster::create(dd_count, rect_larger, rect_larger.xres, rect_larger.yres, 0, GenericRaster::Representation::CPU);
		Raster2D<float> *sum = (Raster2D<float> *) r_sum.get();
		sum->clear(0);
		Raster2D<uint16_t> *count = (Raster2D<uint16_t> *) r_count.get();
		count->clear(0);
		uint16_t count_max = std::numeric_limits<uint16_t>::max()-1;

		auto &vec = points->feature_attributes.numeric(renderattribute);
		for (auto feature : *points) {
			for (auto &p : feature) {
				auto px = sum->WorldToPixelX(p.x);
				auto py = sum->WorldToPixelY(p.y);
				if (px < 0 || py < 0 || px >= sum->width || py >= sum->height)
					continue;

				auto attr = vec.get(feature);
				if (std::isnan(attr))
					continue;

				auto sval = sum->get(px, py);
				sval = sval+attr;
				sum->set(px, py, sval);

				auto cval = count->get(px, py);
				cval = std::min((decltype(cval+1)) count_max, cval+1);
				count->set(px, py, cval);
			}
		}

		Unit unit_result = Unit("unknown", "heatmap"); // TODO: use measurement from the rendered attribute
		unit_result.setMinMax(MIN, MAX);
		unit_result.setInterpolation(Unit::Interpolation::Continuous);
		DataDescription dd_blur(GDT_Float32, unit_result, true, 0);
		auto blurred = GenericRaster::create(dd_blur, rect, rect.xres, rect.yres, 0, GenericRaster::Representation::OPENCL);

		RasterOpenCL::CLProgram prog;
		prog.setProfiler(profiler);
		prog.addInRaster(r_count.get());
		prog.addInRaster(r_sum.get());
		prog.addOutRaster(blurred.get());
		prog.compile(operators_processing_combined_points2raster_value, "blur_value");
		prog.addArg(radius);
		prog.run();

		return blurred;
	}
}
#endif
#endif
