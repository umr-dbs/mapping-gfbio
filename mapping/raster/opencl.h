#ifndef RASTER_OPENCL_H
#define RASTER_OPENCL_H

#ifndef MAPPING_NO_OPENCL
#ifndef MAPPING_OPERATOR_STUBS // STUB operators may not use OpenCL under any circumstance!


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
#include <memory>

#include "datatypes/raster/raster_priv.h"
#include "datatypes/pointcollection.h"

class QueryProfiler;

namespace RasterOpenCL {
	void init();
	void free();
	cl::Platform *getPlatform();
	cl::Context *getContext();
	cl::Device *getDevice();
	cl::CommandQueue *getQueue();

	std::unique_ptr<cl::Buffer> getBufferWithRasterinfo(GenericRaster *raster);

	size_t getMaxAllocSize();

	/**
	 * This class allows the execution of OpenCL kernels on mapping input data
	 */
	class CLProgram {
		public:
			CLProgram();
			~CLProgram();
			void setProfiler(QueryProfiler &_profiler) {
				profiler = &_profiler;
			}
			void addInRaster(GenericRaster *input_raster);
			void addOutRaster(GenericRaster *output_raster);

			size_t addPointCollection(PointCollection *pc);
			void addPointCollectionPositions(size_t idx, bool readonly = false);
			void addPointCollectionAttribute(size_t idx, const std::string &name, bool readonly = false);

			void compile(const std::string &source, const char *kernelname);
			template<typename T> void addArg(T arg) {
				if (!kernel || finished)
					throw OpenCLException("addArg() should only be called between compile() and run()");
				kernel->setArg<T>(argpos++, arg);
			}
			template<typename T> void addArg(std::vector<T> &vec, bool readonly = false) {
				if (!kernel || finished)
					throw OpenCLException("addArg(std::vector) should only be called between compile() and run()");

				size_t size = sizeof(T) * vec.size();

				auto clbuffer = new cl::Buffer(
					*RasterOpenCL::getContext(),
					CL_MEM_USE_HOST_PTR,
					size,
					vec.data()
				);
				scratch_buffers.push_back(clbuffer);
				auto clhostptr = RasterOpenCL::getQueue()->enqueueMapBuffer(*clbuffer, CL_TRUE, CL_MAP_READ | (readonly ? 0 : CL_MAP_WRITE), 0, size);
				scratch_maps.push_back(clhostptr);

				kernel->setArg(argpos++, *clbuffer);
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
			QueryProfiler *profiler;
			// yes, cl::Kernel is something like a shared_ptr around the handles returned by the C API,
			// so why would we want a unique_ptr around a shared_ptr?
			// Because cl::Kernel has no useful public way to check whether it contains a NULL handle.
			std::unique_ptr<cl::Kernel> kernel;
			cl_uint argpos;
			bool finished;
			int iteration_type; // 0 = unknown, 1 = first out_raster, 2 = first pointcollection
			std::vector<GenericRaster *> in_rasters;
			std::vector<GenericRaster *> out_rasters;
			std::vector<PointCollection *> pointcollections;
			std::vector<cl::Buffer *> scratch_buffers;
			std::vector<void *> scratch_maps;
	};
}


#endif // MAPPING_OPERATOR_STUBS

#endif // MAPPING_NO_OPENCL

#endif // RASTER_OPENCL_H
