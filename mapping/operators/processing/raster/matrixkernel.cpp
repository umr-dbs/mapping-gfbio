#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/opencl.h"
#include "operators/operator.h"

#include <memory>
#include <cmath>
#include <json/json.h>


class MatrixOperator : public GenericOperator {
	public:
		MatrixOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MatrixOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
#endif
	protected:
		void writeSemanticParameters(std::ostringstream& stream);
	private:
		int matrixsize, *matrix;
};



MatrixOperator::MatrixOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources), matrixsize(0), matrix(nullptr) {
	assumeSources(1);

	matrixsize = params.get("matrix_size", 0).asInt();
	if (matrixsize <= 1 || matrixsize % 2 != 1)
		throw OperatorException("MatrixKernel: kernel size must be odd and greater than 1");

	Json::Value array = params["matrix"];
	size_t matrix_count = (size_t) matrixsize*matrixsize;
	if (array.size() != matrix_count)
		throw OperatorException("MatrixKernel: matrix array has the wrong length");

	matrix = new int[matrix_count];
	for (size_t i=0;i<matrix_count;i++) {
		matrix[i] = array.get((Json::Value::ArrayIndex) i, 0).asInt();
	}
}

MatrixOperator::~MatrixOperator() {
	delete [] matrix;
	matrix = nullptr;
}
REGISTER_OPERATOR(MatrixOperator, "matrix");

void MatrixOperator::writeSemanticParameters(std::ostringstream& stream) {
	stream << "{\"matrix\":[" << matrix[0];
	for (int i = 1; i < matrixsize; ++i) {
		stream << "," << matrix[i];
	}
	stream << "]}";
}

#ifndef MAPPING_OPERATOR_STUBS
template<typename T> T cap(T v, T min, T max) {
	return std::min(max, std::max(v, min));
}

template<typename T>
struct matrixkernel{
	static std::unique_ptr<GenericRaster> execute(Raster2D<T> *raster_src, int matrix_size, int *matrix) {
		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		auto raster_dest_guard = GenericRaster::create(raster_src->dd, *raster_src, GenericRaster::Representation::CPU);
		Raster2D<T> *raster_dest = (Raster2D<T> *) raster_dest_guard.get();

		auto max = raster_src->dd.unit.getMin();
		auto min = raster_src->dd.unit.getMax();

		int matrix_offset = matrix_size / 2;
		int width = raster_src->width;
		int height = raster_src->height;

		// TODO: Rand getrennt verarbeiten, in der mitte ist kein cap nötig
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				typename RasterTypeInfo<T>::accumulator_type value = 0;
				for (int ky=0;ky<matrix_size;ky++) {
					for (int kx=0;kx<matrix_size;kx++) {
						int source_x = cap(x+kx-matrix_offset, 0, width-1);
						int source_y = cap(y+ky-matrix_offset, 0, height-1);

						value += matrix[ky*matrix_size+kx] * raster_src->get(source_x, source_y);
					}
				}
				if (value > max) value = max;
				if (value < min) value = min;
				raster_dest->set(x, y, value);
			}
		}

		return raster_dest_guard;
	}
};

#include "operators/processing/raster/matrixkernel.cl.h"

std::unique_ptr<GenericRaster> MatrixOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto raster_in = getRasterFromSource(0, rect, profiler);

#ifndef MAPPING_NO_OPENCL
	RasterOpenCL::init();
	raster_in->setRepresentation(GenericRaster::Representation::OPENCL);

	auto raster_out = GenericRaster::create(raster_in->dd, *raster_in, GenericRaster::Representation::OPENCL);

	size_t matrix_count = (size_t) matrixsize*matrixsize;
	size_t matrix_buffer_size = sizeof(matrix[0]) * matrix_count;

	try {
		RasterOpenCL::CLProgram prog;
		prog.setProfiler(profiler);
		prog.addInRaster(raster_in.get());
		prog.addOutRaster(raster_out.get());
		prog.compile(operators_processing_raster_matrixkernel, "matrixkernel");
		prog.addArg((cl_int) matrixsize);

		cl::Buffer matrixbuffer(
			*RasterOpenCL::getContext(),
			CL_MEM_READ_ONLY,
			matrix_buffer_size,
			nullptr //data
		);
		RasterOpenCL::getQueue()->enqueueWriteBuffer(matrixbuffer, CL_TRUE, 0, matrix_buffer_size, matrix);
		prog.addArg(matrixbuffer);
		prog.run();

	}
	catch (cl::Error &e) {
		fprintf(stderr, "cl::Error %d: %s\n", e.err(), e.what());
		throw;
	}

	return raster_out;
#else
	return callUnaryOperatorFunc<matrixkernel>(raster_in.get(), matrixsize, matrix);
#endif
}
#endif
