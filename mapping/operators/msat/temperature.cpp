
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"


#include <limits>
#include <algorithm>
#include <memory>
#include <sstream>
#include <json/json.h>
#include <gdal_priv.h>


class RadianceTable {
	public:
		RadianceTable(int channel, int length, const float *temperatures, const float *radiances)
			: channel(channel), length(length), temperatures(temperatures), radiances(radiances) {

		};
		~RadianceTable() {};

		float getMinTemp() { return temperatures[0]; }
		float getMaxTemp() { return temperatures[length-1]; }
		float getTempFromRadiance(float radiance);
		float getRadianceFromTemp(float temp);
	private:
		int channel;
		int length;
		const float *temperatures;
		const float *radiances;
};

float RadianceTable::getTempFromRadiance(float radiance) {
	if (radiance <= radiances[0])
		return temperatures[0];
	if (radiance >= radiances[length-1])
		return temperatures[length-1];

	for(int i=1;i<length;i++) {
		float prev = radiances[i-1], next = radiances[i];
		if (prev == radiance)
			return temperatures[i];
		if (prev < radiance && next > radiance) {
			// Return a linear interpolation between the two values.
			// The table should be dense enough that this returns reasonably accurate results.
			float l = (next - prev) / (radiance - prev); // [0..1]
			l = std::min(1.0f, std::max(0.0f, l)); // just in case..
			return temperatures[i-1] + l * (temperatures[i]-temperatures[i-1]);
		}
	}

	throw ArgumentException("Radiance not found in table (wtf?)");
}
float RadianceTable::getRadianceFromTemp(float temp) {
	return 0.0;
}


#include "operators/msat/temperature_tables.h"

static RadianceTable *getRadianceTable(int channel) {
	if (channel == 4)
		return &radiancetable_4;
	if (channel == 5)
		return &radiancetable_5;
	if (channel == 6)
		return &radiancetable_6;
	if (channel == 7)
		return &radiancetable_7;
	if (channel == 8)
		return &radiancetable_8;
	if (channel == 9)
		return &radiancetable_9;
	if (channel == 10)
		return &radiancetable_10;
	if (channel == 11)
		return &radiancetable_11;
	throw ArgumentException("getRadianceTable: invalid channel number");
}

/*

Reflektanz:
* für jeden Pixel lat/lon ausrechnen
* Sonnenstand (tag des jahres)? Zenitwinkelkorrektur?
*

 */

class MSATTemperatureOperator : public GenericOperator {
	public:
		MSATTemperatureOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATTemperatureOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
};


#include "operators/msat/temperature.cl.h"



MSATTemperatureOperator::MSATTemperatureOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MSATTemperatureOperator::~MSATTemperatureOperator() {
}
REGISTER_OPERATOR(MSATTemperatureOperator, "msattemperature");


std::unique_ptr<GenericRaster> MSATTemperatureOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect);

	if (raster->dd.min != 0 && raster->dd.max != 1024)
		throw OperatorException("Input raster does not appear to be a meteosat raster");

	int channel = (int) raster->md_value.get("Channel");
	float offset = raster->md_value.get("CalibrationOffset");
	float slope = raster->md_value.get("CalibrationSlope");
	//std::string timestamp = raster->md_string.get("TimeStamp");

	RadianceTable *table = getRadianceTable(channel);

	Profiler::Profiler p1("CL_MSATTEMPERATURE_LOOKUPTABLE");
	float lut[1024];
	for(int i = 0; i < 1024; i++) {
		float radiance = offset + i * slope;
		lut[i] = table->getTempFromRadiance(radiance);
		printf("%d: %f\n", i, lut[i]);
	}

	// migrate the table to openCL
	cl::Buffer lut_buffer(
		*RasterOpenCL::getContext(),
		CL_MEM_READ_ONLY,
		sizeof(lut),
		nullptr //data
	);
	RasterOpenCL::getQueue()->enqueueWriteBuffer(lut_buffer, CL_TRUE, 0, sizeof(lut), lut);


	Profiler::Profiler p("CL_MSATRADIANCE_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	double newmin = table->getMinTemp();
	double newmax = table->getMaxTemp();

	DataDescription out_dd(GDT_Float32, newmin, newmax);
	if (raster->dd.has_no_data) {
		out_dd.addNoData();
		int no_data = (int) raster->dd.no_data;
		if (no_data >= 0 && no_data < 1024)
			lut[no_data] = out_dd.no_data;
	}

	auto raster_out = GenericRaster::create(raster->lcrs, out_dd);

	RasterOpenCL::CLProgram prog;
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_msat_temperature, "temperaturekernel");
	prog.addArg(lut_buffer);
	prog.run();

	return raster_out;
}
