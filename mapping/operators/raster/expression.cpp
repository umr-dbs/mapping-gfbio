#ifndef MAPPING_NO_OPENCL

#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/opencl.h"
#include "operators/operator.h"


#include <limits>
#include <memory>
#include <sstream>
#include <json/json.h>
#include <gdal_priv.h>


class ExpressionOperator : public GenericOperator {
	public:
		ExpressionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~ExpressionOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		std::string expression;
		std::string datatype;
		GDALDataType output_type;
		double output_min, output_max;
};


ExpressionOperator::ExpressionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
	expression = params.get("expression", "value").asString();
	datatype = params.get("datatype", "input").asString();
	if (datatype == "input")
		output_type = GDT_Unknown;
	else {
		output_type = GDALGetDataTypeByName(datatype.c_str()); // Byte, UInt16, Int32, Float32, ..
		if (output_type == GDT_Unknown)
			throw OperatorException(std::string("ExpressionOperator:: Invalid output data type ") + datatype);
	}
	output_min = output_max = 0.0;
	if (params.isMember("min") && params.isMember("max")) {
		output_min = params.get("min", 0.0).asDouble();
		output_max = params.get("max", 0.0).asDouble();
	}
	else
		throw OperatorException("ExpressionOperator always required a value range");
}
ExpressionOperator::~ExpressionOperator() {
}
REGISTER_OPERATOR(ExpressionOperator, "expression");

void ExpressionOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "\"expression\":\"" << expression << "\","
			<< "\"datatype\":\"" << datatype << "\","
			<< "\"min\":" << output_min << ","
			<< "\"max\":" << output_max;
}

std::unique_ptr<GenericRaster> ExpressionOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	int rastercount = getRasterSourceCount();
	if (rastercount < 1 || rastercount > 26)
		throw OperatorException("ExpressionOperator: need between 1 and 26 input rasters");

	RasterOpenCL::init();

	std::vector<std::unique_ptr<GenericRaster> > in_rasters;
	in_rasters.reserve(rastercount);

	// Load all sources and migrate to opencl
	in_rasters.push_back(getRasterFromSource(0, rect, profiler, RasterQM::LOOSE));
	// The first raster determines the data type and sizes
	GenericRaster *raster_in = in_rasters[0].get();
	raster_in->setRepresentation(GenericRaster::OPENCL);

	// figure out the largest time interval common to all input rasters
	TemporalReference tref(raster_in->stref);

	QueryRectangle exact_rect(*raster_in);
	for (int i=1;i<rastercount;i++) {
		in_rasters.push_back(getRasterFromSource(i, exact_rect, profiler, RasterQM::EXACT));
		in_rasters[i]->setRepresentation(GenericRaster::OPENCL);
		tref.intersect(in_rasters[i]->stref);
	}

	/*
	 * Let's assemble our code
	 */
	std::stringstream ss_sourcecode;
	ss_sourcecode << "__kernel void expressionkernel(";
	for (int i=0;i<rastercount;i++) {
		ss_sourcecode << "__global const IN_TYPE" << i << " *in_data" << i << ", __global const RasterInfo *in_info" << i << ",";
	}
	ss_sourcecode << "__global OUT_TYPE0 *out_data, __global const RasterInfo *out_info) {"
		"int gid = get_global_id(0) + get_global_id(1) * in_info0->size[0];"
		"if (gid >= in_info0->size[0]*in_info0->size[1]*in_info0->size[2])"
		"	return;";
	for (int i=0;i<rastercount;i++) {
		char code = 'A' + (char) i;
		ss_sourcecode <<
			"IN_TYPE"<<i<<" "<<code<<" = in_data"<<i<<"[gid];"
			"if (ISNODATA"<<i<<"("<<code<<", in_info"<<i<<")) {"
			"	out_data[gid] = out_info->no_data;"
			"	return;"
			"}";
	}
	ss_sourcecode <<
		"OUT_TYPE0 result = " << expression << ";"
		"out_data[gid] = result;"
		"}";

	std::string sourcecode(ss_sourcecode.str());

	/*
	 * Figure out data type, min and max, and create our output raster
	 */
	GDALDataType output_type = this->output_type;
	if (output_type == GDT_Unknown)
		output_type = raster_in->dd.datatype;

	DataDescription out_dd(output_type, output_min, output_max);
	if (raster_in->dd.has_no_data)
		out_dd.addNoData();

	SpatioTemporalReference out_stref(raster_in->stref, tref);
	auto raster_out = GenericRaster::create(out_dd, out_stref, raster_in->width, raster_in->height, 0, GenericRaster::Representation::OPENCL);

	/*
	 * Run the kernel
	 */
	RasterOpenCL::CLProgram prog;
	prog.setProfiler(profiler);
	for (int i=0;i<rastercount;i++) {
		if (i > 0) {
			if (in_rasters[i]->width != raster_in->width || in_rasters[i]->height != raster_in->height)
				throw OperatorException("ExpressionOperator: not all input rasters have the same dimensions");
		}
		prog.addInRaster(in_rasters[i].get());
	}
	prog.addOutRaster(raster_out.get());
	prog.compile(sourcecode, "expressionkernel");
	prog.run();

	return raster_out;
}

#endif
