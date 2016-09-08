
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"

#include <limits>
#include <memory>
#include <math.h>
#include <json/json.h>
#include <gdal_priv.h>

/**
 * This operator implements black body tmperature correction based on this slideset from Eumetsat: http://eumetrain.org/IntGuide/PowerPoints/Channels/conversion.ppt
* The same method is implemented in SOFOS.
*/
class MeteosatCo2CorrectionOperator : public GenericOperator {
	public:
		MeteosatCo2CorrectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MeteosatCo2CorrectionOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, const QueryTools &tools);
#endif
};




MeteosatCo2CorrectionOperator::MeteosatCo2CorrectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MeteosatCo2CorrectionOperator::~MeteosatCo2CorrectionOperator() {
}
REGISTER_OPERATOR(MeteosatCo2CorrectionOperator, "meteosat_co2_correction");


#ifndef MAPPING_OPERATOR_STUBS
#ifdef MAPPING_NO_OPENCL
std::unique_ptr<GenericRaster> MeteosatCo2CorrectionOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	throw OperatorException("MSATCo2CorrectionOperator: cannot be executed without OpenCL support");
}
#else

#include "operators/processing/meteosat/co2correction.cl.h"

std::unique_ptr<GenericRaster> MeteosatCo2CorrectionOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	RasterOpenCL::init();
	auto raster_bt039 = getRasterFromSource(0, rect, tools, RasterQM::LOOSE);
	QueryRectangle exact_rect(*raster_bt039);
	auto raster_bt108 = getRasterFromSource(1, exact_rect, tools, RasterQM::EXACT);
	auto raster_bt134 = getRasterFromSource(2, exact_rect, tools, RasterQM::EXACT);

	Profiler::Profiler p("CL_MSATCO2CORRECTION_OPERATOR");
	raster_bt039->setRepresentation(GenericRaster::OPENCL);
	raster_bt108->setRepresentation(GenericRaster::OPENCL);
	raster_bt134->setRepresentation(GenericRaster::OPENCL);

	//TODO: check if raster lcrs are equal
	DataDescription out_dd(GDT_Float32, raster_bt039->dd.unit); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster_bt039->dd.has_no_data||raster_bt108->dd.has_no_data||raster_bt134->dd.has_no_data)
		out_dd.addNoData();
	auto raster_out = GenericRaster::create(out_dd, *raster_bt039, GenericRaster::Representation::OPENCL);

	RasterOpenCL::CLProgram prog;
	prog.addInRaster(raster_bt039.get());
	prog.addInRaster(raster_bt108.get());
	prog.addInRaster(raster_bt134.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_processing_meteosat_co2correction, "co2correctionkernel");
	prog.run();

	return raster_out;
}
#endif
#endif
