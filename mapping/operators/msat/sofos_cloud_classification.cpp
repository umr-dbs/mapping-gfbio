
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "util/sunpos.h"
#include "sofos_constants.h"



#include <limits>
#include <memory>
#include <math.h>
//#include <sstream>
#include <ctime>        // struct std::tm
//#include <time.h>
//#include <iterator>
#include <json/json.h>
#include <gdal_priv.h>


class MsatSofosCloudClassificationOperator : public GenericOperator {
	public:
		MsatSofosCloudClassificationOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MsatSofosCloudClassificationOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect);
	private:
};


#include "operators/msat/classification_kernels.cl.h"


MsatSofosCloudClassificationOperator::MsatSofosCloudClassificationOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MsatSofosCloudClassificationOperator::~MsatSofosCloudClassificationOperator() {
}
REGISTER_OPERATOR(MsatSofosCloudClassificationOperator, "msatsofoscloudclassification");

std::unique_ptr<GenericRaster> MsatSofosCloudClassificationOperator::getRaster(const QueryRectangle &rect) {
	RasterOpenCL::init();

	auto raster_solar_zenith_angle = getRasterFromSource(0, rect);

	Profiler::Profiler p("CL_MSAT_SOFOS_CLOUD_CLASSIFICATION_OPERATOR");

	/*SOFOS STEP 1: Detect if day or night!*/

	//create the output raster using the no_data value of the cloud classification
	const DataDescription out_dd(GDT_UInt16, cloudclass::is_surface, cloudclass::range_illumination, true, 0.0);
	auto raster_out = GenericRaster::create(raster_solar_zenith_angle->lcrs, out_dd);

	//define classification and move it to OpenCL
	std::string kernelName = "multiThresholdKernel";
	std::vector<uint16_t> classifications{cloudclass::is_no_data, 0, cloudclass::is_twilight, cloudclass::is_day|cloudclass::is_twilight, cloudclass::is_day, cloudclass::is_day|cloudclass::is_twilight, cloudclass::is_twilight, 0};
	std::vector<float> classification_thresholds{-360.0, -100.0, -90.0, -80.0, 80.0, 90.0, 100.0, 360.0};

	cl::Buffer cl_buffer1{*RasterOpenCL::getContext(), CL_MEM_READ_ONLY, sizeof(uint16_t)*classifications.size()};
	RasterOpenCL::getQueue()->enqueueWriteBuffer(cl_buffer1, CL_TRUE, 0, sizeof(uint16_t)*classifications.size(), classifications.data());

	cl::Buffer cl_buffer2{*RasterOpenCL::getContext(), CL_MEM_READ_ONLY, sizeof(float)*classification_thresholds.size()};
	RasterOpenCL::getQueue()->enqueueWriteBuffer(cl_buffer2, CL_TRUE, 0, sizeof(float)*classification_thresholds.size(), classification_thresholds.data());

	//set the solar zenith angle raster to opencl
	raster_solar_zenith_angle->setRepresentation(GenericRaster::OPENCL);

	//create and run the first test
	RasterOpenCL::CLProgram dayNightDetection;
	dayNightDetection.addInRaster(raster_solar_zenith_angle.get());
	dayNightDetection.addOutRaster(raster_out.get());
	dayNightDetection.compile(operators_msat_classification_kernels, kernelName.c_str());
	dayNightDetection.addArg(cl_buffer2);
	dayNightDetection.addArg(cl_buffer1);
	dayNightDetection.addArg(static_cast<int>(classification_thresholds.size()));
	dayNightDetection.run();


	/*SOFOS STEP 2: Detect if day or night!*/




	return raster_out;
}
