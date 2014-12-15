#ifndef RASTER_OPENCL_H
#define RASTER_OPENCL_H

#ifndef MAPPING_NO_OPENCL

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

#include "raster/raster_priv.h"
#include "raster/pointcollection.h"

namespace RasterOpenCL {
	void init();
	void free();
	cl::Platform *getPlatform();
	cl::Context *getContext();
	cl::Device *getDevice();
	cl::CommandQueue *getQueue();

	cl::Kernel addProgramFromFile(const char *filename, const char *kernelname);
	cl::Kernel addProgram(const std::string &sourcecode, const char *kernelname);

	cl::Buffer *getBufferWithRasterinfo(GenericRaster *raster);


	class CLProgram {
		public:
			CLProgram();
			~CLProgram();
			void addInRaster(GenericRaster *input_raster);
			void addOutRaster(GenericRaster *output_raster);

			size_t addPointCollection(PointCollection *pc);
			void addPointCollectionPositions(size_t idx);
			void addPointCollectionAttribute(size_t idx, const std::string &name);

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
				if (!kernel || finished)
					throw OpenCLException("addArg() should only be called between compile() and run()");
				kernel->setArg(argpos++, buffer);
			}
			void run();
			cl::Event run(std::vector<cl::Event>* events_to_wait_for);
			void reset();
		private:
			void cleanScratch();
			cl::Kernel *kernel;
			cl_uint argpos;
			bool finished;
			int iteration_type; // 0 = unknown, 1 = first in_raster, 2 = first pointcollection
			std::vector<GenericRaster *> in_rasters;
			std::vector<GenericRaster *> out_rasters;
			std::vector<PointCollection *> pointcollections;
			std::vector<cl::Buffer *> scratch_buffers;
			std::vector<void *> scratch_maps;
	};
}


#endif

#endif
