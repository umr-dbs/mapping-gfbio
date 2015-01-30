
#include "raster/raster.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "sofos_constants.h"
#include "plot/histogram.h"


#include <memory>
#include <math.h>
//#include <time.h>
//#include <iterator>
#include <json/json.h>

class MSATGccThermThresholdDetectionOperator : public GenericOperator {
	public:
		MSATGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params);
		virtual ~MSATGccThermThresholdDetectionOperator();

		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, QueryProfiler &profiler);
};


//#include "operators/msat/co2correction.cl.h"

template<typename T1, typename T2>
struct CreateHistogramPairFunction{
	static std::pair<Histogram, Histogram> execute(Raster2D<T1> *bt108_minus_bt039_raster, Raster2D<T2> *sza_raster, int bucket_scale, double sza_day_min, double sza_day_max, double sza_night_min, double sza_night_max) {
		bt108_minus_bt039_raster->setRepresentation(GenericRaster::Representation::CPU);
		sza_raster->setRepresentation(GenericRaster::Representation::CPU);

	   //get min and max
		double bt108_minus_bt039_min = bt108_minus_bt039_raster->dd.min;
		double bt108_minus_bt039_max = bt108_minus_bt039_raster->dd.max;

		auto bt108_minus_bt039_raster_range = RasterTypeInfo<T1>::getRange(bt108_minus_bt039_min, bt108_minus_bt039_max);

		auto buckets = bt108_minus_bt039_raster_range*bucket_scale;
		Histogram day_histogram = Histogram{static_cast<int>(buckets), static_cast<double>(bt108_minus_bt039_min), static_cast<double>(bt108_minus_bt039_max)};
		Histogram night_histogram = Histogram{static_cast<int>(buckets), static_cast<double>(bt108_minus_bt039_min), static_cast<double>(bt108_minus_bt039_max)};

		int size_a = bt108_minus_bt039_raster->lcrs.getPixelCount();
		int size_b = sza_raster->lcrs.getPixelCount();

		if(size_a != size_b){
			//do something here
		}

	   for (int i=0;i<size_a;i++) {
		   T1 value = bt108_minus_bt039_raster->data[i];
		   T2 condition = sza_raster->data[i];
		   if (bt108_minus_bt039_raster->dd.is_no_data(value) || sza_raster->dd.is_no_data(condition)){
			   day_histogram.incNoData();
		   	   night_histogram.incNoData();
		   }
		   else if(condition >= sza_day_min && condition <= sza_day_max) {
			   day_histogram.inc(value);
		   	   night_histogram.incNoData();
		   }
		   else if(condition >= sza_night_min && condition <= sza_night_max){
			   day_histogram.incNoData();
			   night_histogram.inc(value);
		   }
		   else{ //solar zenith angle indicates twilight
			   day_histogram.incNoData();
			   night_histogram.incNoData();
		   }
	   }
	   return std::pair<Histogram,Histogram>{day_histogram, night_histogram};
	}
};

template<typename T1, typename T2>
struct CreateConditionalHistogramFunction{
	static std::unique_ptr<Histogram> execute(Raster2D<T1> *value_raster, Raster2D<T2> *condition_raster, int bucket_scale, double condition_min, double condition_max) {
		value_raster->setRepresentation(GenericRaster::Representation::CPU);
		condition_raster->setRepresentation(GenericRaster::Representation::CPU);

	   //get min and max
		T1 bt108_minus_bt039_min = (T1) value_raster->dd.min;
		T1 bt108_minus_bt039_max = (T1) value_raster->dd.max;

		auto bt108_minus_bt039_raster_range = RasterTypeInfo<T1>::getRange(bt108_minus_bt039_min, bt108_minus_bt039_max);

		auto buckets = bt108_minus_bt039_raster_range*bucket_scale;

		std::cerr<<"min: "<<bt108_minus_bt039_min<<" |max: "<<bt108_minus_bt039_max<<" |range: "<<bt108_minus_bt039_raster_range<<" |buckets:"<<buckets<<std::endl;

		auto histogram_ptr = std::make_unique<Histogram>(buckets, bt108_minus_bt039_min, bt108_minus_bt039_max);

		int size_a = value_raster->lcrs.getPixelCount();
		int size_b = condition_raster->lcrs.getPixelCount();

		if(size_a != size_b){
			//do something here
		}

	   for (int i=0;i<size_a;i++) {
		   T1 value = value_raster->data[i];
		   T2 condition = condition_raster->data[i];
		   if (value_raster->dd.is_no_data(value) || condition_raster->dd.is_no_data(condition)){
			   histogram_ptr->incNoData();
		   }
		   else if(condition >= condition_min && condition <= condition_max) {
			   histogram_ptr->inc(value);
		   }
		   else{
			   histogram_ptr->incNoData();
		   }
	   }

	   return histogram_ptr;
	}
};


int findGccThermThreshold(Histogram &histogram, float min, float max){
	const float minimum_land_peak_temperature = std::numeric_limits<float>::min();
	const int minimum_decreasing_bins_before_cloud_threshold = 10;

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

	return minimum_between_land_and_cloud_peak_bucket;
}

MSATGccThermThresholdDetectionOperator::MSATGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MSATGccThermThresholdDetectionOperator::~MSATGccThermThresholdDetectionOperator() {
}
REGISTER_OPERATOR(MSATGccThermThresholdDetectionOperator, "msatgccthermthresholddetection");



std::unique_ptr<GenericPlot> MSATGccThermThresholdDetectionOperator::getPlot(const QueryRectangle &rect, QueryProfiler &profiler) {
	auto solar_zenith_angle_raster = getRasterFromSource(0, rect, profiler);
	auto bt108_minus_bt039_raster = getRasterFromSource(1, rect, profiler);

	Profiler::Profiler p("MSATGCCTHERMTHRESHOLDDETECTION_OPERATOR");
	solar_zenith_angle_raster->setRepresentation(GenericRaster::CPU);
	bt108_minus_bt039_raster->setRepresentation(GenericRaster::CPU);

	//TODO: check if raster lcrs are equal
	DataDescription out_dd(GDT_Float32, bt108_minus_bt039_raster->dd.min, bt108_minus_bt039_raster->dd.max); // no no_data //raster->dd.has_no_data, output_no_data);
	if (bt108_minus_bt039_raster->dd.has_no_data)
		out_dd.addNoData();
	auto raster_out = GenericRaster::create(bt108_minus_bt039_raster->lcrs, out_dd);

	auto histogram_ptr = callBinaryOperatorFunc<CreateConditionalHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), 3, cloudclass::day_solar_zenith_angle_min, cloudclass::day_solar_zenith_angle_max);
	
	
	return std::unique_ptr<GenericPlot>(std::move(histogram_ptr));
}
