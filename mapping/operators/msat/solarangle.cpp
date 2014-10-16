
#include "raster/raster.h"
#include "raster/typejuggling.h"
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

enum class SolarAngles {AZIMUTH, ZENITH};

class MSATSolarAngleOperator : public GenericOperator {
	public:
		MSATSolarAngleOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATSolarAngleOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
		SolarAngles solarAngle;
};


#include "operators/msat/solarangle.cl.h"


MSATSolarAngleOperator::MSATSolarAngleOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	std::string specifiedAngle = params.get("solarangle", "none").asString();
	if (specifiedAngle == "azimuth")
		solarAngle = SolarAngles::AZIMUTH;
	else if (specifiedAngle == "zenith")
		solarAngle = SolarAngles::ZENITH;
		else {
			solarAngle = SolarAngles::AZIMUTH;
			throw OperatorException(std::string("MSATSolarAngleOperator:: Invalid SolarAngle specified: ") + specifiedAngle);
	}
}
MSATSolarAngleOperator::~MSATSolarAngleOperator() {
}
REGISTER_OPERATOR(MSATSolarAngleOperator, "msatsolarangle");

std::unique_ptr<GenericRaster> MSATSolarAngleOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect);

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

	//TODO: Channel12 would use 65536 / -40927014 * 1000.134348869 = -1.601074451590×10^-6. The difference is: 1.93384285×10^-9
	double projectionCooridnateToViewAngleFactor = 65536 / (-13642337.0 * 3004.03165817); //= -1.59914060874×10^-6

	Profiler::Profiler p("CL_MSAT_SOLARANGLE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	//
	DataDescription out_dd(GDT_Float32, 0.0, 360.0); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(raster->lcrs, out_dd);

	std::string kernelName;
	if(solarAngle == SolarAngles::AZIMUTH)
		kernelName = "azimuthKernel";
	else if(solarAngle == SolarAngles::ZENITH)
		kernelName = "zenithKernel";
	else
		throw OperatorException(std::string("MSATSolarAngleOperator:: Trying to initiate OpenCL kernel for an invalid SolarAngle!"));

	RasterOpenCL::CLProgram prog;
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_msat_solarangle, kernelName.c_str());
	prog.addArg(projectionCooridnateToViewAngleFactor);
	prog.addArg(psaIntermediateValues.dGreenwichMeanSiderealTime);
	prog.addArg(psaIntermediateValues.dRightAscension);
	prog.addArg(psaIntermediateValues.dDeclination);
	prog.run();

	return raster_out;
}
