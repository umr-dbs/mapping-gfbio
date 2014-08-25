
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "util/sunpos.h"


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
	strptime(timestamp.c_str(),"%Y%m%d%H%M", &timeDate);
	std::cerr<<"timeDate.tm_mday:"<<timeDate.tm_mday<<"|timeDate.tm_mon:"<<timeDate.tm_mon<<"|timeDate.tm_year:"<<timeDate.tm_year<<"|timeDate.tm_hour:"<<timeDate.tm_hour<<"|timeDate.tm_min:"<<timeDate.tm_min<<"|ORIGINAL:"<<timestamp<<std::endl;

	//now calculate the intermediate values using PSA algorithm
	cIntermediateVariables psaIntermediateValues = sunposIntermediate(timeDate.tm_year+1900, timeDate.tm_mon+1,	timeDate.tm_mday, timeDate.tm_hour, timeDate.tm_min, 0.0);
	std::cerr<<"GMST: "<<psaIntermediateValues.dGreenwichMeanSiderealTime<<" dRightAscension: "<<psaIntermediateValues.dRightAscension<<" dDeclination: "<<psaIntermediateValues.dDeclination<<std::endl;

	//Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	//raster->setRepresentation(GenericRaster::OPENCL);

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
