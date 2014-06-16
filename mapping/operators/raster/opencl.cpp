
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

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};



OpenCLOperator::OpenCLOperator(int sourcecount, GenericOperator *sources[], Json::Value &params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
OpenCLOperator::~OpenCLOperator() {
}
REGISTER_OPERATOR(OpenCLOperator, "opencl");


GenericRaster *OpenCLOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();
	GenericRaster *raster = sources[0]->getRaster(rect);
	std::unique_ptr<GenericRaster> raster_guard(raster);

	Profiler::Profiler p("CL_OPERATOR");

	raster->setRepresentation(GenericRaster::OPENCL);

	GenericRaster *raster2 = GenericRaster::create(raster->rastermeta, raster->valuemeta);
	std::unique_ptr<GenericRaster> raster2_guard(raster2);
	raster2->setRepresentation(GenericRaster::OPENCL);


	cl::Kernel kernel = RasterOpenCL::addProgramFromFile("operators/cl/test.cl", "testKernel");
	kernel.setArg(0, *raster->getCLBuffer());
	kernel.setArg(1, *raster2->getCLBuffer());
	kernel.setArg(2, (int) raster->rastermeta.size[0]);
	kernel.setArg(3, (int) raster->rastermeta.size[1]);
	//kernel.setArg(2, (int) raster->rastermeta.getPixelCount());


	cl::Event event;
	try {
		Profiler::Profiler p("CL_EXECUTE");
		RasterOpenCL::getQueue()->enqueueNDRangeKernel(kernel,
			cl::NullRange, // Offset
			cl::NDRange(raster->rastermeta.getPixelCount()), // Global
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
	return raster2_guard.release();
}

