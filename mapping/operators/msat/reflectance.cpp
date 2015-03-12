
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "util/sunpos.h"



#include <limits>
#include <memory>
#include <math.h>
//#include <sstream>
#include <ctime>        // struct std::tm
//#include <time.h>
//#include <iterator>
#include <json/json.h>
#include <gdal_priv.h>


class MSATReflectanceOperator : public GenericOperator {
	public:
		MSATReflectanceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATReflectanceOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
		bool solarCorrection{true};
};


#include "operators/msat/reflectance.cl.h"



MSATReflectanceOperator::MSATReflectanceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MSATReflectanceOperator::~MSATReflectanceOperator() {
}
REGISTER_OPERATOR(MSATReflectanceOperator, "msatreflectance");


double calculateDevisorFor(int channel, int dayOfYear) {
	int index = 0;
	if(channel>=0 && channel <=2)
		index = channel;
	else if(channel == 12)
		index = 3;
	else
		throw ArgumentException("MSG: radiance only valid for channels 1-3 and 12!");

	//calculate ESD?
	double dESD = 1.0 - 0.0167 * cos(2.0 * acos(-1.0) * ((dayOfYear - 3.0) / 365.0));
	/*
	//get the central wavelength
	double cwl = msg::dCwl[channel - 1];
	// calculate the ETSR?
	double etsr = msg::dETSRconst[index] * 10 / (dESD*dESD) * (cwl*cwl);
	//now we can precalculate the devisior we need to change radiance to reflectance
	double div = (cwl*cwl) * etsr;
	// reflectance is radiance * 10 / div
	return 10/div;
	*/

	//y not zoidberg? //TODO check if 1/something produces a problem
	return 1 / msg::dETSRconst[index] / (dESD*dESD);
}

double calculateESD(int dayOfYear){
	return 1.0 - 0.0167 * cos(2.0 * acos(-1.0) * ((dayOfYear - 3.0) / 365.0));
}

std::unique_ptr<GenericRaster> MSATReflectanceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	// get the timestamp of the MSG scene from the raster metadata
	std::string timestamp = raster->md_string.get("TimeStamp");
	// create and store a real time object
	std::tm timeDate;

	/** TODO: This would be the c++11 way to do it. Sadly this is not implemented in GCC 4.8...
	std::istringstream ss(timestamp);
	ss >> std::get_time(&time, "%Y%m%d%H%M%S");
	*/
	strptime(timestamp.c_str(),"%Y%m%d%H%M", &timeDate);
	//std::cerr<<"timeDate.tm_mday:"<<timeDate.tm_mday<<"|timeDate.tm_mon:"<<timeDate.tm_mon<<"|timeDate.tm_year:"<<timeDate.tm_year<<"|timeDate.tm_hour:"<<timeDate.tm_hour<<"|timeDate.tm_min:"<<timeDate.tm_min<<"|ORIGINAL:"<<timestamp<<std::endl;

	//now calculate the intermediate values using PSA algorithm
	cIntermediateVariables psaIntermediateValues = sunposIntermediate(timeDate.tm_year+1900, timeDate.tm_mon+1,	timeDate.tm_mday, timeDate.tm_hour, timeDate.tm_min, 0.0);
	//std::cerr<<"GMST: "<<psaIntermediateValues.dGreenwichMeanSiderealTime<<" dRightAscension: "<<psaIntermediateValues.dRightAscension<<" dDeclination: "<<psaIntermediateValues.dDeclination<<std::endl;

	/* DEBUG infos
	//get more information about the raster dimensions of the processed tile
	LocalCRS lcrs = raster->lcrs;
	std::cerr<<"epsg:"<<lcrs.epsg<<"|dimensions:"<<lcrs.dimensions<<"|origin:";
    //print the origin
	std::copy(std::begin(lcrs.origin),
    		  std::end(lcrs.origin),
              std::ostream_iterator<double>(std::cerr,",")
             );
	std::cerr<<"direct:"<<lcrs.origin[0]<<lcrs.origin[1]<<lcrs.origin[2]<<std::endl;
	//print the scale
	std::cerr<<"|scale:";
	std::copy(std::begin(lcrs.scale),
    		  std::end(lcrs.scale),
              std::ostream_iterator<double>(std::cerr,",")
             );
	//print the size
	std::cerr<<"|size:";
	std::copy(std::begin(lcrs.size),
    		  std::end(lcrs.size),
              std::ostream_iterator<int>(std::cerr,",")
             );
	std::cerr<<std::endl;
	*/

	// now we need the channelNumber to calculate ETSR and ESD
	int channel = (int) raster->md_value.get("Channel");
	double dETSRconst = msg::dETSRconst[channel];
	double dESD = calculateESD(timeDate.tm_yday+1);
	//std::cerr<<"channel:"<<channel<<"|dETSRconst"<<dETSRconst<<"|dESD:"<<dESD<<std::endl;

	//TODO: Channel12 would use 65536 / -40927014 * 1000.134348869 = -1.601074451590�10^-6. The difference is: 1.93384285�10^-9
	double projectionCooridnateToViewAngleFactor = 65536 / (-13642337.0 * 3004.03165817); //= -1.59914060874�10^-6


	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	//
	DataDescription out_dd(GDT_Float32, -0.1, 1.2); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(out_dd, *raster, GenericRaster::Representation::OPENCL);

	RasterOpenCL::CLProgram prog;
	prog.setProfiler(profiler);
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	if(solarCorrection){
		prog.compile(operators_msat_reflectance, "reflectanceWithSolarCorrectionKernel");
		prog.addArg(psaIntermediateValues.dGreenwichMeanSiderealTime);
		prog.addArg(psaIntermediateValues.dRightAscension);
		prog.addArg(psaIntermediateValues.dDeclination);
		prog.addArg(projectionCooridnateToViewAngleFactor);
	}
	else{
		prog.compile(operators_msat_reflectance, "reflectanceWithoutSolarCorrectionKernel");
	}
	prog.addArg(dETSRconst);
	prog.addArg(dESD);
	prog.run();

	return raster_out;
}
