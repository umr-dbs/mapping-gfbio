
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "util/sunpos.h"
#include "sofos_constants.h"
#include "datatypes/plots/histogram.h"



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

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
	private:
};


#include "operators/msat/classification_kernels.cl.h"

template<typename T1, typename T2>
struct RasterDifferenceHistogramFunction{
	static Histogram execute(Raster2D<T1> *raster_a, Raster2D<T2> *raster_b, int bucket_scale) {
	   raster_a->setRepresentation(GenericRaster::Representation::CPU);
	   raster_b->setRepresentation(GenericRaster::Representation::CPU);

	   //approximate max min
	   T1 max = (T1) raster_a->dd.max - (T2) raster_b->dd.min;
	   T2 min = (T1) raster_a->dd.min - (T2) raster_b->dd.max;

	   auto range_a = RasterTypeInfo<T1>::getRange(min, max);
	   auto range_b = RasterTypeInfo<T1>::getRange(min, max);
	   auto range = std::max(range_a, range_b);

	   auto buckets = range*bucket_scale;
	   Histogram histogram = Histogram{static_cast<int>(buckets), static_cast<double>(min), static_cast<double>(max)};

	   int size_a = raster_a->getPixelCount();
	   int size_b = raster_b->getPixelCount();

	   if(size_a != size_b){
		   //do something here
	   }

	   for (int i=0;i<size_a;i++) {
		   T1 a = raster_a->data[i];
		   T2 b = raster_b->data[i];
		   if (raster_a->dd.is_no_data(a) || raster_b->dd.is_no_data(b))
			   histogram.incNoData();
		   else {
			   histogram.inc(a-b);
		   }
	   }
	   return histogram;
	}
};

int findGccThermThreshold(Histogram &histogram, float min, float max){
	float minimum_land_peak_temperature = std::numeric_limits<float>::min();
	int minimum_decreasing_bins_before_cloud_threshold = 10;

	/**first we need to find the land peak**/
	int land_peak_bucket = 0;
	//the landpeak is not allowed to be lower than minimum_land_peak_temperature
	if(histogram.getMax() < minimum_land_peak_temperature){
	   land_peak_bucket = histogram.getNumberOfBuckets();
	}
	else {
	   //now we are sure that there is a bucket inside the histogram, witch contains minimum_land_peak_temperature
	   land_peak_bucket = histogram.calculateBucketForValue(minimum_land_peak_temperature);
	   int land_peak_count = histogram.getCountForBucket(land_peak_bucket);
	   //now we will find the absolute maximum (with #bucket >= land_peak_bucket) in the histogram which is the real land peak (TODO: literature)
	   for(int i = land_peak_bucket+1; i < histogram.getNumberOfBuckets(); i++){
		   const int tempCount = histogram.getCountForBucket(i);
		   if(tempCount > land_peak_count){
			   land_peak_count = tempCount;
			   land_peak_bucket = i;
		   }
	   }
	}

	/**second we need to identify the minimum with (with #bucket < land_peak_bucket)**/
	int minimum_between_land_and_cloud_peak_bucket = land_peak_bucket;
	//int minimum_between_land_and_cloud_peak_bucket_count = 0;
	int decreasing_buckets = 0;

	for(int i = land_peak_bucket-1; i >= 0; i--){
	   int bucket_count = histogram.getCountForBucket(i);

	   if(bucket_count < histogram.getCountForBucket(i+1)){
		   decreasing_buckets += 1;
		   //if(bucket_count < minimum_between_land_and_cloud_peak_bucket_count){
			   minimum_between_land_and_cloud_peak_bucket = i;
			   //minimum_between_land_and_cloud_peak_bucket_count = bucket_count;
		   //}
	   }
	   else if(decreasing_buckets >= minimum_decreasing_bins_before_cloud_threshold){
		   break;
	   }
	   else{
		   decreasing_buckets = 0;
		   }
	   }

	/**now we know the bucket of the cloud threshold :) **/




}

MsatSofosCloudClassificationOperator::MsatSofosCloudClassificationOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MsatSofosCloudClassificationOperator::~MsatSofosCloudClassificationOperator() {
}
REGISTER_OPERATOR(MsatSofosCloudClassificationOperator, "msatsofoscloudclassification");


std::unique_ptr<GenericRaster> MsatSofosCloudClassificationOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	RasterOpenCL::init();

	Profiler::Profiler p("CL_MSAT_SOFOS_CLOUD_CLASSIFICATION_OPERATOR");

	/*SOFOS STEP 1: Detect if day or night!*/
	auto raster_solar_zenith_angle = getRasterFromSource(0, rect, profiler);

	//create the output raster using the no_data value of the cloud classification
	const DataDescription out_dd(GDT_UInt16, cloudclass::is_surface, cloudclass::range_illumination, true, 0.0);
	auto raster_out = GenericRaster::create(out_dd, *raster_solar_zenith_angle, GenericRaster::Representation::OPENCL);

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


	/*SOFOS STEP 2: Find the "grosstherm" threshold for (day/night) cloud detection*/
	auto raster_bt039 = getRasterFromSource(1, rect, profiler);
	raster_bt039->setRepresentation(GenericRaster::CPU);

	auto raster_bt108 = getRasterFromSource(2, rect, profiler);
	raster_bt108->setRepresentation(GenericRaster::CPU);

	Histogram histogram = callBinaryOperatorFunc<RasterDifferenceHistogramFunction>(raster_bt108.get(), raster_bt039.get(), 3);
	histogram.getNoDataCount();




	return raster_out;
}
