
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"

#include <limits>
#include <memory>
#include <sstream>
#include <json/json.h>
#include <gdal_priv.h>


class MeteosatRadianceOperator : public GenericOperator {
	public:
		MeteosatRadianceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MeteosatRadianceOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, const QueryTools &tools);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	//private:
	//	bool convert; // indicates if radiance will be converted to W/ �m m� sr
};




MeteosatRadianceOperator::MeteosatRadianceOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);

	//this->convert = params.get("conversion", false).asBool();
}

MeteosatRadianceOperator::~MeteosatRadianceOperator() {
}

REGISTER_OPERATOR(MeteosatRadianceOperator, "meteosat_radiance");


void MeteosatRadianceOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{}";
	//stream << "\"convert\":" << (convert ? "true" : "false");
}


#ifndef MAPPING_OPERATOR_STUBS
#ifdef MAPPING_NO_OPENCL
std::unique_ptr<GenericRaster> MeteosatRadianceOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	throw OperatorException("MSATRadianceOperator: cannot be executed without OpenCL support");
}
#else

#include "operators/processing/meteosat/radiance.cl.h"

std::unique_ptr<GenericRaster> MeteosatRadianceOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	RasterOpenCL::init();
	auto raster = getRasterFromSource(0, rect, tools);

	if (raster->dd.unit.getMeasurement() != "raw" || !raster->dd.unit.hasMinMax())
		throw OperatorException("Input raster does not appear to be a raw meteosat raster");

	float offset = raster->global_attributes.getNumeric("msg.CalibrationOffset");
	float slope = raster->global_attributes.getNumeric("msg.CalibrationSlope");

	raster->setRepresentation(GenericRaster::OPENCL);

	double newmin = offset + raster->dd.unit.getMin() * slope;
	double newmax = offset + raster->dd.unit.getMax() * slope;
	float conversionFactor = 1.0f;

	/*
	if(this->convert){
		float dCwl = msg::dCwl[channel];
		conversionFactor = 10.0f /(dCwl*dCwl);
	}
	*/

	Unit out_unit("radiance", "W·m^(-2)·sr^(-1)·cm^(-1)");
	out_unit.setMinMax(newmin, newmax);
	out_unit.setInterpolation(Unit::Interpolation::Continuous);
	DataDescription out_dd(GDT_Float32, out_unit); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(out_dd, *raster, GenericRaster::Representation::OPENCL);

	RasterOpenCL::CLProgram prog;
	prog.setProfiler(tools.profiler);
	prog.addInRaster(raster.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(operators_processing_meteosat_radiance, "radianceConvertedKernel");
	prog.addArg(offset);
	prog.addArg(slope);
	prog.addArg(conversionFactor);
	prog.run();

	raster_out->global_attributes = raster->global_attributes;

	return raster_out;
}
#endif
#endif
