
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



/**
 * This function uses the approximation method published by Eumetsat to calculate BTs from Radiance.
 * https://www.eumetsat.int/website/wcm/idc/idcplg?IdcService=GET_FILE&dDocName=PDF_EFFECT_RAD_TO_BRIGHTNESS&RevisionSelectionMethod=LatestReleased&Rendition=Web
 * @param wavenumber the wavenumber of a channel
 * @param alpha the alpha constant of a channel
 * @param beta the beta constant of a channel
 * @param radiance the radiance for which the BT should be computed
 * @return BT
 */
static double calculateTempFromEffectiveRadiance(double wavenumber, double alpha, double beta, double radiance) {
	double temp = (msg::c1 * 1.0e6 * wavenumber*wavenumber*wavenumber) / (1.0e-5 * radiance);
	temp = ((msg::c2* 100. * wavenumber / std::log(temp + 1)) - beta) / alpha;
	return temp;
}

void MSATTemperatureOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{\"forceSatellite\":" << "\"" << forceSatellite << "\"}";
}

std::unique_ptr<GenericRaster> MSATTemperatureOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, profiler);

	if (raster->dd.unit.getMeasurement() != "raw" || raster->dd.unit.getMin() != 0 || raster->dd.unit.getMax() != 1023)
		throw OperatorException("Input raster does not appear to be a raw meteosat raster");

	msg::Satellite satellite;
	if(forceSatellite.length() > 0){
		satellite = msg::getSatelliteForName(forceSatellite);
	}
	else{
		int satellite_id = (int) raster->global_attributes.getNumeric("msg.Satellite");
		satellite = msg::getSatelliteForMsgId(satellite_id);
	}

	int channel = (int) raster->global_attributes.getNumeric("msg.Channel");

	if (channel < 3 || channel >10)
		throw OperatorException("BT calculation is only valid for Channels 4-11");

	float offset = raster->global_attributes.getNumeric("msg.CalibrationOffset");
	float slope = raster->global_attributes.getNumeric("msg.CalibrationSlope");

	double wavenumber = satellite.vc[channel];
	double alpha = satellite.alpha[channel];
	double beta = satellite.beta[channel];

	Profiler::Profiler p1("CL_MSATTEMPERATURE_LOOKUPTABLE");
	std::vector<float> lut;
	lut.reserve(1024);
	for(int i = 0; i < 1024; i++) {
		float radiance = offset + i * slope;
		float temperature = (float) calculateTempFromEffectiveRadiance(wavenumber, alpha, beta, radiance);
		lut.push_back(temperature);
	}

	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	//TODO: find min/max in the lut or use min/max of the input data...
	double newmin = 200;//table->getMinTemp();
	double newmax = 330;//table->getMaxTemp();

	Unit out_unit("temperature", "k");
	out_unit.setMinMax(newmin, newmax);
	out_unit.setInterpolation(Unit::Interpolation::Continuous);
	DataDescription out_dd(GDT_Float32, out_unit);
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
