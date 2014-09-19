#ifndef MAPPING_NO_OPENCL

#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
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

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
		std::string expression;
		GDALDataType output_type;
		bool has_manual_value_range;
		double output_min, output_max;
};



#include "operators/raster/expression_header.cl.h"
#include "operators/raster/expression_footer.cl.h"


template<typename T>
struct isIntegerType {
	static bool execute(Raster2D<T> *) {
		return RasterTypeInfo<T>::isinteger;
	}
};


ExpressionOperator::ExpressionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
	expression = params.get("expression", "value").asString();
	std::string datatype = params.get("datatype", "input").asString();
	if (datatype == "input")
		output_type = GDT_Unknown;
	else {
		output_type = GDALGetDataTypeByName(datatype.c_str()); // Byte, UInt16, Int32, Float32, ..
		if (output_type == GDT_Unknown)
			throw OperatorException(std::string("ExpressionOperator:: Invalid output data type ") + datatype);
	}
	has_manual_value_range = false;
	output_min = output_max = 0.0;
	if (params.isMember("min") && params.isMember("max")) {
		has_manual_value_range = true;
		output_min = params.get("min", 0.0).asDouble();
		output_max = params.get("max", 0.0).asDouble();
	}
}
ExpressionOperator::~ExpressionOperator() {
}
REGISTER_OPERATOR(ExpressionOperator, "expression");


template<typename T>
struct createValueRaster {
	static std::unique_ptr<GenericRaster> execute(Raster2D<T> *raster) {
		auto range = RasterTypeInfo<T>::getRange(raster->dd.min, raster->dd.max);
		T min = (T) raster->dd.min;
		T max = (T) raster->dd.max;
		T no_data = (T) raster->dd.no_data;
		if (raster->dd.has_no_data && no_data >= min && no_data <= max)
			range--;
		LocalCRS value_rm(EPSG_UNKNOWN, range, 1,  0.0, 0.0, 1.0, 1.0);
		auto value_raster_guard = GenericRaster::create(value_rm, raster->dd);
		Raster2D<T> *value_raster = (Raster2D<T> *) value_raster_guard.get();

		uint32_t x = 0;
		// the extra condition protects against cases, where we're iterating 0..255 on uint8
		for (T value = min; value <= max && value != (T) (max+1); value++) {
			if (raster->dd.has_no_data && no_data == value)
				continue;
			if (x >= value_rm.size[0]) {
				fprintf(stderr, "%f -> %f, nodata(%d) = %f, value = %f, x = %u, max = %u", (float) min, (float) max, raster->dd.has_no_data ? 1 : 0, (float) no_data, (float) value, x, value_rm.size[0]);
				throw OperatorException("Internal Error: createValueRaster() is bugged, call a bug hunter.");
			}
			value_raster->set(x++, 0, value);
		}

		return value_raster_guard;
	}
};

template<typename T>
struct getActualMinMax {
	static void execute(Raster2D<T> *raster, double *out_min, double *out_max) {
		T actual_min = std::numeric_limits<T>::max();
		T actual_max = std::numeric_limits<T>::min();

		for (uint32_t x=0;x<raster->lcrs.size[0];x++) {
			T value = raster->get(x, 0);
			actual_min = std::min(actual_min, value);
			actual_max = std::max(actual_max, value);
		}

		*out_min = (double) actual_min;
		*out_max = (double) actual_max;
	}
};


std::unique_ptr<GenericRaster> ExpressionOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	auto raster_in = getRasterFromSource(0, rect);

	if (!has_manual_value_range) {
		if (!callUnaryOperatorFunc<isIntegerType>(raster_in.get()))
			throw OperatorException("ExpressionOperator on floating point values required a manual value range");

		double drange = raster_in->dd.max - raster_in->dd.min;
		if (drange > 4096 || drange <= 0)
			throw OperatorException("ExpressionOperator on raster with range > 4096 required a manual value range");
	}

	GDALDataType output_type = this->output_type;
	if (output_type == GDT_Unknown)
		output_type = raster_in->dd.datatype;

	DataDescription output_dd(output_type, raster_in->dd.min, raster_in->dd.max);

	Profiler::Profiler p("CL_EXPRESSION_OPERATOR");
	raster_in->setRepresentation(GenericRaster::OPENCL);

	/*
	 * So, first things first: let's assemble our code
	 */
	std::stringstream ss_sourcecode;
	ss_sourcecode << operators_raster_expression_header;
	ss_sourcecode << expression;
	ss_sourcecode << operators_raster_expression_footer;

	std::string sourcecode(ss_sourcecode.str());


	/**
	 * Maybe we need to figure out the new value range, i.e. dd.min and dd.max
	 * Unless we want to pick the formula apart, differentiate and find minima and maxima, we'll just brute force
	 * We'll fill a (range x 1) raster with all values from min .. max, apply the expression,
	 * then find the smallest and largest value.
	 */
	double newmin, newmax;
	if (!has_manual_value_range) {
		auto value_raster = callUnaryOperatorFunc<createValueRaster>(raster_in.get());
		auto value_output_raster = GenericRaster::create(value_raster->lcrs, output_dd, GenericRaster::Representation::OPENCL);

		RasterOpenCL::CLProgram prog;
		prog.addInRaster(value_raster.get());
		prog.addOutRaster(value_output_raster.get());
		prog.compile(sourcecode, "expressionkernel");
		prog.run();

		value_raster.reset();

		// TODO: we could of course try to get the min/max via opencl..
		value_output_raster->setRepresentation(GenericRaster::CPU);

		callUnaryOperatorFunc<getActualMinMax>(value_output_raster.get(), &newmin, &newmax);
	}
	else {
		newmin = output_min;
		newmax = output_max;
	}

	/*
	 * Now that we got our new min and max.. time to do this.
	 */
	DataDescription out_dd(output_type, newmin, newmax);
	if (raster_in->dd.has_no_data)
		out_dd.addNoData();

	auto raster_out = GenericRaster::create(raster_in->lcrs, out_dd);

	RasterOpenCL::CLProgram prog;
	prog.addInRaster(raster_in.get());
	prog.addOutRaster(raster_out.get());
	prog.compile(sourcecode, "expressionkernel");
	prog.run();

	return raster_out;
}

#endif
