
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


enum class SolarAngles {AZIMUTH, ZENITH};

class MeteosatSolarAngleOperator : public GenericOperator {
	public:
		MeteosatSolarAngleOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MeteosatSolarAngleOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		SolarAngles solarAngle;
};


MeteosatSolarAngleOperator::MeteosatSolarAngleOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	std::string specifiedAngle = params.get("solarangle", "none").asString();
	if (specifiedAngle == "azimuth")
		solarAngle = SolarAngles::AZIMUTH;
	else if (specifiedAngle == "zenith")
		solarAngle = SolarAngles::ZENITH;
	else
		throw OperatorException(std::string("MSATSolarAngleOperator:: Invalid SolarAngle specified: ") + specifiedAngle);
}
MeteosatSolarAngleOperator::~MeteosatSolarAngleOperator() {
}
REGISTER_OPERATOR(MeteosatSolarAngleOperator, "meteosat_solar_angle");

void MeteosatSolarAngleOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{";
	stream << "\"solarangle\":";
	if (solarAngle == SolarAngles::AZIMUTH)
		stream << "\"azimuth\"";
	else if (solarAngle == SolarAngles::ZENITH)
		stream << "\"zenith\"";
	stream << "}";
}

#ifndef MAPPING_OPERATOR_STUBS
#ifdef MAPPING_NO_OPENCL
std::unique_ptr<GenericRaster> MeteosatSolarAngleOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	throw OperatorException("MSATSolarAngleOperator: cannot be executed without OpenCL support");
}
#else

#include "operators/processing/meteosat/solarangle.cl.h"

std::unique_ptr<GenericRaster> MeteosatSolarAngleOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	// TODO: do we have any requirement for the input raster?

	// get the timestamp of the MSG scene from the raster metadata
	std::string timestamp = raster->global_attributes.getTextual("msg.TimeStamp");
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

	//TODO: Channel12 would use 65536 / -40927014 * 1000.134348869 = -1.601074451590�10^-6. -> should be identical...
	//x = X * 65536 / (CFAC * ColumnDirGridStep)
	double projectionCooridnateToViewAngleFactor = 65536 / (-13642337 * 3000.403165817);

	Profiler::Profiler p("CL_MSAT_SOLARANGLE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	//
	Unit out_unit("solarangle", "degree");
	out_unit.setMinMax(0.0, 360.0);
	DataDescription out_dd(GDT_Float32, out_unit); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(out_dd, *raster, GenericRaster::Representation::OPENCL);

	std::string kernelName;
	if(solarAngle == SolarAngles::AZIMUTH)
		kernelName = "azimuthKernel";
	else if(solarAngle == SolarAngles::ZENITH)
		kernelName = "zenithKernel";
	else
		throw OperatorException(std::string("MSATSolarAngleOperator:: Trying to initiate OpenCL kernel for an invalid SolarAngle!"));

	RasterOpenCL::CLProgram prog;
	prog.setProfiler(profiler);
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
#endif
#endif
