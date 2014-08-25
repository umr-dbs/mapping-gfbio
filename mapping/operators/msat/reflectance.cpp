
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
//#include "util/SunPos.h"


#include <limits>
#include <memory>
#include <sstream>
#include <ctime>        // struct std::tm
#include <time.h>
#include <json/json.h>
#include <gdal_priv.h>


class MSATReflectanceOperator : public GenericOperator {
	public:
		MSATReflectanceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~MSATReflectanceOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
};


//#include "operators/msat/reflectance.cl.h"



MSATReflectanceOperator::MSATReflectanceOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
MSATReflectanceOperator::~MSATReflectanceOperator() {
}
REGISTER_OPERATOR(MSATReflectanceOperator, "msatreflectance");


std::unique_ptr<GenericRaster> MSATReflectanceOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	auto raster = sources[0]->getRaster(rect);

	// get the timestamp of the MSG scene from the raster metadata
	std::string timestamp = raster->md_string.get("TimeStamp");
	// create and store a real time object
	std::tm timeDate;

	/** TODO: This would be the c++11 way to do it. Sadly GCC does not implement it...
	std::istringstream ss(timestamp);
	ss >> std::get_time(&time, "%Y%m%d%H%M%S");
	*/
	strptime(timestamp.c_str(),"%y-%m-%d %H:%M", &timeDate);



	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
//	raster->setRepresentation(GenericRaster::OPENCL);
//
//	double newmin = offset + raster->dd.min * slope;
//	double newmax = offset + raster->dd.max * slope;
//
//	DataDescription out_dd(GDT_Float32, newmin, newmax); // no no_data //raster->dd.has_no_data, output_no_data);
//	if (raster->dd.has_no_data)
//		out_dd.addNoData();
//
//	auto raster_out = GenericRaster::create(raster->lcrs, out_dd);
//
//	RasterOpenCL::CLProgram prog;
//	prog.addInRaster(raster.get());
//	prog.addOutRaster(raster_out.get());
//	prog.compile(operators_msat_radiance, "radiancekernel");
//	prog.addArg(offset);
//	prog.addArg(slope);
//	prog.run();

	return raster;
}
