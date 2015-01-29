
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"


#include <limits>
#include <memory>
#include <sstream>
#include <json/json.h>
#include <gdal_priv.h>


class MSATRadianceOperator : public GenericOperator {
	public:
		MSATRadianceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATRadianceOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		bool convert; // indicates if radiance will be converted to W/ µm m² sr
};


#include "operators/msat/radiance.cl.h"



MSATRadianceOperator::MSATRadianceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	std::string conversion = params.get("conversion", "false").asString();
	this->convert = (conversion == "true");

}
MSATRadianceOperator::~MSATRadianceOperator() {
}
// We do not need this operator any more, the raster source can scale automatically.
REGISTER_OPERATOR(MSATRadianceOperator, "msatradiance");


std::unique_ptr<GenericRaster> MSATRadianceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	float offset = raster->md_value.get("CalibrationOffset");
	float slope = raster->md_value.get("CalibrationSlope");
	int channel = (int) raster->md_value.get("Channel");


	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	double newmin = offset + raster->dd.min * slope;
	double newmax = offset + raster->dd.max * slope;
	float conversionFactor = 1.0f;

	if(this->convert){
		float dCwl = msg::dCwl[channel];
		conversionFactor = 10.0f /(dCwl*dCwl);
	}

	DataDescription out_dd(GDT_Float32, newmin, newmax); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(raster->lcrs, out_dd);

	RasterOpenCL::CLProgram prog;
	prog.setProfiler(profiler);
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_msat_radiance, "radianceConvertedKernel");
	prog.addArg(offset);
	prog.addArg(slope);
	prog.addArg(conversionFactor);
	prog.run();

	return raster_out;
}
