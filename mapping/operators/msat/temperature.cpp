
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "operators/msat/msg_constants.h"


#include <vector>
#include <limits>
#include <algorithm>
#include <cmath>
#include <memory>
#include <sstream>
#include <json/json.h>
#include <gdal_priv.h>


class MSATTemperatureOperator : public GenericOperator {
	public:
		MSATTemperatureOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATTemperatureOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		std::string forceSatellite;

};
MSATTemperatureOperator::MSATTemperatureOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	forceSatellite = params.get("forceSatellite", "").asString();
}
MSATTemperatureOperator::~MSATTemperatureOperator() {
}
REGISTER_OPERATOR(MSATTemperatureOperator, "msattemperature");


#ifndef MAPPING_OPERATOR_STUBS

#include "operators/msat/temperature.cl.h"

static double calculateTempFromEffectiveRadiance(double wavenumber, double alpha, double beta, double radiance) {
	double temp = (msg::c1 * 1.0e6 * wavenumber*wavenumber*wavenumber) / (1.0e-5 * radiance);
	temp = ((msg::c2* 100. * wavenumber / std::log(temp + 1)) - beta) / alpha;
	return temp;
}

void MSATTemperatureOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << ", \"forceSatellite\":" << "\"" << forceSatellite << "\"";
}

std::unique_ptr<GenericRaster> MSATTemperatureOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	if (raster->dd.min != 0 && raster->dd.max != 1024)
		throw OperatorException("Input raster does not appear to be a meteosat raster");

	msg::Satellite satellite;
	if(forceSatellite.length() > 0){
		satellite = msg::getSatelliteForName(forceSatellite);
	}
	else{
		std::string satellite_id_str = raster->md_string.get("Satellite");
		int satellite_id = std::stoi(satellite_id_str);
		satellite = msg::getSatelliteForMsgId(satellite_id);
	}

	int channel = (int) raster->md_value.get("Channel");

	if (channel < 3 || channel >10)
		throw OperatorException("BT calculation is only valid for Channels 4-11");

	float offset = raster->md_value.get("CalibrationOffset");
	float slope = raster->md_value.get("CalibrationSlope");
	//std::string timestamp = raster->md_string.get("TimeStamp");

	double wavenumber = satellite.vc[channel];
	double alpha = satellite.alpha[channel];
	double beta = satellite.beta[channel];

	Profiler::Profiler p1("CL_MSATTEMPERATURE_LOOKUPTABLE");
	std::vector<float> lut;
	lut.reserve(1024);
	for(int i = 0; i < 1024; i++) {
		float radiance = offset + i * slope;
		float temperature = calculateTempFromEffectiveRadiance(wavenumber, alpha, beta, radiance);
		lut.push_back(temperature);
	}

	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	double newmin = 200;//table->getMinTemp();
	double newmax = 330;//table->getMaxTemp();

	DataDescription out_dd(GDT_Float32, newmin, newmax);
	if (raster->dd.has_no_data) {
		out_dd.addNoData();
		int no_data = (int) raster->dd.no_data;
		if (no_data >= 0 && no_data < 1024)
			lut[no_data] = out_dd.no_data;
	}

	auto raster_out = GenericRaster::create(out_dd, *raster, GenericRaster::Representation::OPENCL);

	RasterOpenCL::CLProgram prog;
	prog.setProfiler(profiler);
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_msat_temperature, "temperaturekernel");
	prog.addArg(lut);
	prog.run();

	return raster_out;
}
#endif
