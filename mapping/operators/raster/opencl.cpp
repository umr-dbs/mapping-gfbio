
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"


#include <memory>
#include <json/json.h>
#include <gdal_priv.h>


class OpenCLOperator : public GenericOperator {
	public:
		OpenCLOperator(int sourcecount, GenericOperator *sources[], Json::Value &params);
		virtual ~OpenCLOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
};



OpenCLOperator::OpenCLOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
OpenCLOperator::~OpenCLOperator() {
}
REGISTER_OPERATOR(OpenCLOperator, "opencl");


std::unique_ptr<GenericRaster> OpenCLOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	auto raster_in = sources[0]->getRaster(rect);

	Profiler::Profiler p("CL_OPERATOR");
	raster_in->setRepresentation(GenericRaster::OPENCL);

	auto raster_out = GenericRaster::create(raster_in->lcrs, raster_in->dd, GenericRaster::OPENCL);


	cl::Kernel kernel = RasterOpenCL::addProgramFromFile("operators/cl/test.cl", "testKernel");
	kernel.setArg(0, *raster_in->getCLBuffer());
	kernel.setArg(1, *raster_out->getCLBuffer());
	kernel.setArg(2, (int) raster_in->lcrs.size[0]);
	kernel.setArg(3, (int) raster_in->lcrs.size[1]);
	//kernel.setArg(2, (int) raster->lcrs.getPixelCount());


	cl::Event event;
	try {
		Profiler::Profiler p("CL_EXECUTE");
		RasterOpenCL::getQueue()->enqueueNDRangeKernel(kernel,
			cl::NullRange, // Offset
			cl::NDRange(raster_in->lcrs.getPixelCount()), // Global
			cl::NullRange, // cl::NDRange(1, 1), // local
			nullptr, //events
			&event //event
		);
	}
	catch (cl::Error &e) {
		printf("Error %d: %s\n", e.err(), e.what());

		throw;
	}

	event.wait();
	return raster_out;
}
