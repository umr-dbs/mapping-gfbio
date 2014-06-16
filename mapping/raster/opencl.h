#ifndef RASTER_OPENCL_H
#define RASTER_OPENCL_H

//#define __NO_STD_VECTOR // Use cl::vector instead of STL version
#include <vector>
#define __CL_ENABLE_EXCEPTIONS
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.hpp>
#endif

#include <string>
#include <gdal_priv.h>

namespace RasterOpenCL {
	void init();
	void free();
	cl::Platform *getPlatform();
	cl::Context *getContext();
	cl::Device *getDevice();
	cl::CommandQueue *getQueue();

	cl::Kernel addProgramFromFile(const char *filename, const char *kernelname);
	cl::Kernel addProgram(const std::string &sourcecode, const char *kernelname);

	//void setKernelArgByGDALType(cl::Kernel &kernel, cl_uint arg, GDALDataType datatype, double value);
	//void setKernelArgAsRasterinfo(cl::Kernel &kernel, cl_uint arg, GenericRaster *raster);
	cl::Buffer *getBufferWithRasterinfo(GenericRaster *raster);
	//const std::string &getRasterInfoStructSource();

	//void runGenericTwoRasterKernel(cl::Kernel &kernel, GenericRaster *in, GenericRaster *out);



	class CLProgram {
		public:
			CLProgram();
			~CLProgram();
			void addInRaster(GenericRaster *input_raster);
			void addOutRaster(GenericRaster *output_raster);
			void compile(const std::string &source, const char *kernelname);
			void compileFromFile(const char *filename, const char *kernelname);
			template<typename T> void addArg(T arg) {
				if (!kernel || finished)
					throw OpenCLException("addArg() should only be called between compile() and run()");
				kernel->setArg<T>(argpos++, arg);
			}
			void addArg(size_t size, void *argPtr) {
				if (!kernel || finished)
					throw OpenCLException("addArg() should only be called between compile() and run()");
				kernel->setArg(argpos++, size, argPtr);
			}
			void addArg(cl::Buffer &buffer) {
				kernel->setArg(argpos++, buffer);
			}
			void run();
			void reset();
		private:
			cl::Kernel *kernel;
			cl_uint argpos;
			bool finished;
			std::vector<GenericRaster *> in_rasters;
			std::vector<GenericRaster *> out_rasters;
	};
}


#endif
