
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
#include <ctime>  // struct std::tm
#include <string>
#include <json/json.h>
#include <gdal_priv.h>


class MSATReflectanceOperator : public GenericOperator {
	public:
		MSATReflectanceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATReflectanceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		bool solarCorrection;
		bool forceHRV;
		std::string forceSatellite;
};


#include "operators/msat/reflectance.cl.h"



MSATReflectanceOperator::MSATReflectanceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	solarCorrection = params.get("solarCorrection", true).asBool();
	forceHRV = params.get("forceHRV", false).asBool();
	forceSatellite = params.get("forceSatellite", "").asString();
}
MSATReflectanceOperator::~MSATReflectanceOperator() {
}
REGISTER_OPERATOR(MSATReflectanceOperator, "msatreflectance");

void MSATReflectanceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	stream << "\"solarCorrection\":" << (solarCorrection ? "true" : "false");
	stream << ", \"forceHRV\":" << (forceHRV ? "true" : "false");
	stream << ", \"forceSatellite\":" << "\"" << forceSatellite << "\"";
	stream << "}";
}


#ifndef MAPPING_OPERATOR_STUBS

/**
 * This function calculates the earth sun distance for a given day of year
 * @param dayOfYear day of year
 * @return earth sun distance
 */
double calculateESD(int dayOfYear){
	return 1.0 - 0.0167 * cos(2.0 * acos(-1.0) * ((dayOfYear - 3.0) / 365.0));
}

std::unique_ptr<GenericRaster> MSATReflectanceOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	if (raster->dd.unit.getMeasurement() != "radiance" || raster->dd.unit.getUnit() != "W·m^(-2)·sr^(-1)·cm^(-1)")
		throw OperatorException("Input raster does not appear to be a meteosat radiance raster");

	// get all the metadata:
	int channel = (forceHRV) ? 11 : (int) raster->global_attributes.getNumeric("msg.Channel");
	std::string timestamp = raster->global_attributes.getTextual("msg.TimeStamp");

	msg::Satellite satellite;

	if(forceSatellite.length() > 0){
		satellite = msg::getSatelliteForName(forceSatellite);
	}
	else{
		int satellite_id = (int) raster->global_attributes.getNumeric("msg.Satellite");
		satellite = msg::getSatelliteForMsgId(satellite_id);
	}


	// create and store a real time object
	std::tm timeDate;
	/** TODO: This would be the c++11 way to do it. Sadly this is not implemented in GCC 4.8...
	std::istringstream ss(timestamp);
	ss >> std::get_time(&time, "%Y%m%d%H%M%S");
	*/
	strptime(timestamp.c_str(),"%Y%m%d%H%M", &timeDate);

	//now calculate the intermediate values using PSA algorithm
	cIntermediateVariables psaIntermediateValues = sunposIntermediate(timeDate.tm_year+1900, timeDate.tm_mon+1,	timeDate.tm_mday, timeDate.tm_hour, timeDate.tm_min, 0.0);

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


	// get extra terrestrial solar radiation (...) and ESD (solar position)
	double etsr = satellite.etsr[channel]/M_PI;
	double esd = calculateESD(timeDate.tm_yday+1);

	//calculate the projection to viewangle factor:
	// channel 01-10 (1-11) = -13642337.0 * 3000.403165817
	// channel 11 (12 = HRV)= -40927014.0 * 1000.134348869
	double projectionCooridnateToViewAngleFactor = 65536 / ((channel == 11)? (-40927014 * 1000.134348869) : (-13642337 * 3000.403165817)); //= -1.59914060874�10^-6


	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	//
	Unit out_unit("reflectance", "fraction");
	out_unit.setMinMax(-0.1, 1.2); // TODO: ??? Shouldn't this be between 0 and 1?
	out_unit.setInterpolation(Unit::Interpolation::Continuous);
	DataDescription out_dd(GDT_Float32, out_unit); // no no_data //raster->dd.has_no_data, output_no_data);
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
	prog.addArg(etsr);
	prog.addArg(esd);
	prog.run();

	return raster_out;
}
#endif
