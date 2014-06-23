
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
		ExpressionOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~ExpressionOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
	private:
		std::string expression;
		GDALDataType output_type;
};


/*
static std::string StringReplaceAll(std::string subject, const std::string& search, const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
         subject.replace(pos, search.length(), replace);
         pos += replace.length();
    }
    return subject;
}
*/

#include "operators/raster/expression_header.cl.h"
#include "operators/raster/expression_header_nodata.cl.h"
#include "operators/raster/expression_footer.cl.h"


template<typename T>
struct getCLTypeName {
	static const char *execute(Raster2D<T> *) {
		return RasterTypeInfo<T>::cltypename;
	}
};

template<typename T>
struct isIntegerType {
	static bool execute(Raster2D<T> *) {
		return RasterTypeInfo<T>::isinteger;
	}
};


ExpressionOperator::ExpressionOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
	expression = params.get("expression", "value").asString();
	output_type = GDALGetDataTypeByName(params.get("datatype", "x").asString().c_str()); // Byte, UInt16, Int32, Float32, ..
}
ExpressionOperator::~ExpressionOperator() {
}
REGISTER_OPERATOR(ExpressionOperator, "expression");


template<typename T>
struct createValueRaster {
	static GenericRaster *execute(Raster2D<T> *raster) {
		auto range = RasterTypeInfo<T>::getRange(raster->dd.min, raster->dd.max);
		T min = (T) raster->dd.min;
		T max = (T) raster->dd.max;
		T no_data = (T) raster->dd.no_data;
		if (raster->dd.has_no_data && no_data >= min && no_data <= max)
			range--;
		LocalCRS value_rm(EPSG_UNKNOWN, range, 1,  0.0, 0.0, 1.0, 1.0);
		Raster2D<T> *value_raster = (Raster2D<T> *) GenericRaster::create(value_rm, raster->dd);
		std::unique_ptr<GenericRaster> value_raster_guard(value_raster);

		uint32_t x = 0;
		// the extra condition protects against cases, where we're iterating 0..255 on uint8
		for (T value = min; value <= max && value != (T) (max+1); value++) {
			if (raster->dd.has_no_data && no_data == value)
				continue;
			if (x >= value_rm.size[0]) {
				fprintf(stderr, "%d -> %d, nodata(%d) = %d, value = %d, x = %d, max = %d", min, max, raster->dd.has_no_data ? 1 : 0, no_data, value, x, value_rm.size[0]);
				throw OperatorException("Internal Error: createValueRaster() is bugged, call a bug hunter.");
			}
			value_raster->set(x++, 0, value);
		}

		return value_raster_guard.release();
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


GenericRaster *ExpressionOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	GenericRaster *raster = sources[0]->getRaster(rect);
	std::unique_ptr<GenericRaster> raster_guard(raster);


	if (!callUnaryOperatorFunc<isIntegerType>(raster))
		throw OperatorException("Cannot use ExpressionOperator on floating point values yet (TODO)");

	double drange = raster->dd.max - raster->dd.min;
	if (drange > 4096 || drange <= 0)
		throw OperatorException("Cannot use ExpressionOperator on raster with range > 4096 yet (TODO)");

	GDALDataType output_type = this->output_type;
	if (output_type == GDT_Unknown)
		output_type = raster->dd.datatype;

	DataDescription output_dd(output_type, raster->dd.min, raster->dd.max);

	Profiler::Profiler p("CL_EXPRESSION_OPERATOR");
	raster->setRepresentation(GenericRaster::OPENCL);

	/*
	 * So, first things first: let's assemble our code
	 */
	std::stringstream ss_sourcecode;
	if (raster->dd.has_no_data)
		ss_sourcecode << operators_raster_expression_header_nodata;
	else
		ss_sourcecode << operators_raster_expression_header;
	ss_sourcecode << expression;
	ss_sourcecode << operators_raster_expression_footer;

	std::string sourcecode(ss_sourcecode.str());

	/**
	 * We need to figure out the new value range, i.e. dd.min and dd.max
	 * Unless we want to pick the formula apart, differentiate and fine minima and maxima, we'll just brute force
	 * We'll fill a (range x 1) raster with all values from 0 .. range-1, apply the expression,
	 * then find the smallest and largest value.
	 */
	GenericRaster *value_raster = callUnaryOperatorFunc<createValueRaster>(raster);
	std::unique_ptr<GenericRaster> value_raster_guard(value_raster);

	GenericRaster *value_output_raster = GenericRaster::create(value_raster->lcrs, output_dd, GenericRaster::Representation::OPENCL);
	std::unique_ptr<GenericRaster> value_output_raster_guard(value_output_raster);


	RasterOpenCL::CLProgram prog;
	prog.addInRaster(value_raster);
	prog.addOutRaster(value_output_raster);
	prog.compile(sourcecode, "expressionkernel");
	prog.run();

	value_raster_guard.reset();
	value_raster = nullptr;

	// TODO: we could of course try to get the min/max via opencl..
	value_output_raster->setRepresentation(GenericRaster::CPU);

	double newmin = raster->dd.min;
	double newmax = raster->dd.max;
	callUnaryOperatorFunc<getActualMinMax>(value_output_raster, &newmin, &newmax);

	value_output_raster_guard.reset();
	value_output_raster = nullptr;


	/*
	 * Now that we got our new min and max.. time to do this.
	 */
	DataDescription out_dd(output_type, newmin, newmax); // no no_data //raster->dd.has_no_data, output_no_data);
	if (raster->dd.has_no_data)
		out_dd.addNoData();

	GenericRaster *raster_out = GenericRaster::create(raster->lcrs, out_dd);
	std::unique_ptr<GenericRaster> raster_out_guard(raster_out);

	prog.reset();
	prog.addInRaster(raster);
	prog.addOutRaster(raster_out);
	prog.compile(sourcecode, "expressionkernel");
	prog.run();

	return raster_out_guard.release();
}
