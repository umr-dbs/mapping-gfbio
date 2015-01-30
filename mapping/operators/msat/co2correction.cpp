
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"

#include <limits>
#include <memory>
#include <math.h>
//#include <sstream>
#include <ctime>        // struct std::tm
//#include <time.h>
//#include <iterator>
#include <json/json.h>
#include <gdal_priv.h>


class MSATCo2CorrectionOperator : public GenericOperator {
	public:
		MSATCo2CorrectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATCo2CorrectionOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		bool solarCorrection{true};
};


#include "operators/msat/co2correction.cl.h"



MSATCo2CorrectionOperator::MSATCo2CorrectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MSATCo2CorrectionOperator::~MSATCo2CorrectionOperator() {
}
REGISTER_OPERATOR(MSATCo2CorrectionOperator, "msatco2correction");



std::unique_ptr<GenericRaster> MSATCo2CorrectionOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster_bt039 = getRasterFromSource(0, rect, profiler);
	auto raster_bt108 = getRasterFromSource(1, rect, profiler);
	auto raster_bt134 = getRasterFromSource(2, rect, profiler);

	Profiler::Profiler p("CL_MSATCO2CORRECTION_OPERATOR");
	raster_bt039->setRepresentation(GenericRaster::OPENCL);
	raster_bt108->setRepresentation(GenericRaster::OPENCL);
	raster_bt134->setRepresentation(GenericRaster::OPENCL);

	//TODO: check if raster lcrs are equal
	DataDescription out_dd(GDT_Float32, raster_bt039->dd.min, raster_bt039->dd.max); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster_bt039->dd.has_no_data||raster_bt108->dd.has_no_data||raster_bt134->dd.has_no_data)
		out_dd.addNoData();
	auto raster_out = GenericRaster::create(raster_bt039->lcrs, out_dd);

	RasterOpenCL::CLProgram prog;
	prog.addInRaster(raster_bt039.get());
	prog.addInRaster(raster_bt108.get());
	prog.addInRaster(raster_bt134.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_msat_co2correction, "co2correctionkernel");
	prog.run();

	return raster_out;
}
