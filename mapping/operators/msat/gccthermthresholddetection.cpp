
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "sofos_constants.h"
#include "datatypes/plots/histogram.h"

#include <iostream>
#include <fstream>

#include <memory>
#include <math.h>
//#include <time.h>
//#include <iterator>
#include <json/json.h>

class MSATGccThermThresholdDetectionOperator : public GenericOperator {
	public:
		MSATGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &/*params*/);
		virtual ~MSATGccThermThresholdDetectionOperator();

		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, QueryProfiler &profiler);
};


#include "operators/msat/classification_kernels.cl.h"


//template<typename T1, typename T2>
//struct CreateHistogramPairFunction{
//	static std::pair<Histogram, Histogram> execute(Raster2D<T1> *bt108_minus_bt039_raster, Raster2D<T2> *sza_raster, int bucket_scale, double sza_day_min, double sza_day_max, double sza_night_min, double sza_night_max) {
//		bt108_minus_bt039_raster->setRepresentation(GenericRaster::Representation::CPU);
//		sza_raster->setRepresentation(GenericRaster::Representation::CPU);
//
//	   //get min and max
//		double bt108_minus_bt039_min = bt108_minus_bt039_raster->dd.min;
//		double bt108_minus_bt039_max = bt108_minus_bt039_raster->dd.max;
//
//		auto bt108_minus_bt039_raster_range = RasterTypeInfo<T1>::getRange(bt108_minus_bt039_min, bt108_minus_bt039_max);
//
//		auto buckets = bt108_minus_bt039_raster_range*bucket_scale;
//		Histogram day_histogram = Histogram{static_cast<int>(buckets), static_cast<double>(bt108_minus_bt039_min), static_cast<double>(bt108_minus_bt039_max)};
//		Histogram night_histogram = Histogram{static_cast<int>(buckets), static_cast<double>(bt108_minus_bt039_min), static_cast<double>(bt108_minus_bt039_max)};
//
//		int size_a = bt108_minus_bt039_raster->getPixelCount();
//		int size_b = sza_raster->getPixelCount();
//
//		if(size_a != size_b){
//			//do something here
//		}
//
//	   for (int i=0;i<size_a;i++) {
//		   T1 value = bt108_minus_bt039_raster->data[i];
//		   T2 condition = sza_raster->data[i];
//		   if (bt108_minus_bt039_raster->dd.is_no_data(value) || sza_raster->dd.is_no_data(condition)){
//			   day_histogram.incNoData();
//		   	   night_histogram.incNoData();
//		   }
//		   else if(condition >= sza_day_min && condition <= sza_day_max) {
//			   day_histogram.inc(value);
//		   	   night_histogram.incNoData();
//		   }
//		   else if(condition >= sza_night_min && condition <= sza_night_max){
//			   day_histogram.incNoData();
//			   night_histogram.inc(value);
//		   }
//		   else{ //solar zenith angle indicates twilight
//			   day_histogram.incNoData();
//			   night_histogram.incNoData();
//		   }
//	   }
//	   return std::pair<Histogram,Histogram>{day_histogram, night_histogram};
//	}
//};

template<typename T1, typename T2>
struct CreateConditionalHistogramFunction{
	static std::unique_ptr<Histogram> execute(Raster2D<T1> *value_raster, Raster2D<T2> *condition_raster, double bucket_size, double condition_min, double condition_max) {
		value_raster->setRepresentation(GenericRaster::Representation::CPU);
		condition_raster->setRepresentation(GenericRaster::Representation::CPU);

	   //get min and max
		T1 value_raster_min = (T1) value_raster->dd.min;
		T1 value_raster_max = (T1) value_raster->dd.max;

		auto value_raster_range = RasterTypeInfo<T1>::getRange(value_raster_min, value_raster_max);

		auto buckets = std::ceil(value_raster_range/bucket_size);

		std::cerr<<"min: "<<value_raster_min<<" |max: "<<value_raster_max<<" |range: "<<value_raster_range<<" |buckets:"<<buckets<<std::endl;

		auto histogram_ptr = std::make_unique<Histogram>(buckets, value_raster_min, value_raster_max);

		int size_a = value_raster->getPixelCount();
		int size_b = condition_raster->getPixelCount();

		if(size_a != size_b){
			//do something here
		}

	   for (int i=0;i<size_a;i++) {
		   T1 value = value_raster->data[i];
		   T2 condition_value = condition_raster->data[i];
		   if (value_raster->dd.is_no_data(value) || condition_raster->dd.is_no_data(condition_value)){
			   histogram_ptr->incNoData();
		   }
		   else if(condition_value >= condition_min && condition_value < condition_max) {
			   histogram_ptr->inc(value);
		   }
		   else{
			   histogram_ptr->incNoData();
		   }
	   }

	   return histogram_ptr;
	}
};


double findGccThermThreshold(Histogram &histogram, const double minimum_land_peak_temperature, const int minimum_increasing_bins_for_rising_trend){

	/**start: find the land peak**/

	//start with the minimum
	double land_peak_temperature = minimum_land_peak_temperature;
	int land_peak_bucket = -1;

	//if there are buckets above the minimum_land_peak_temperature search the max -> real land peak
	if(histogram.getMax() > minimum_land_peak_temperature){
		land_peak_bucket = histogram.calculateBucketForValue(minimum_land_peak_temperature);
		int land_peak_count = histogram.getCountForBucket(land_peak_bucket);

		for(int i = land_peak_bucket; i < histogram.getNumberOfBuckets(); i++){
			int tempCount = histogram.getCountForBucket(i);
			if(tempCount > land_peak_count){
				land_peak_count = tempCount;
				land_peak_bucket = i;
			}
		}
		land_peak_temperature = histogram.calculateBucketLowerBorder(land_peak_bucket);
	}
	histogram.addMarker(land_peak_temperature, "landpeak: "+std::to_string(land_peak_temperature)+" bucket: "+std::to_string(land_peak_bucket));
	/**end: find the land peak*/


	/**second we need to find a rising trend (indicated by **/
	int minimum_between_land_and_cloud_peak_bucket = land_peak_bucket;
	//int minimum_between_land_and_cloud_peak_bucket_count = 0;
	int increasing_buckets = 0;

	for(int i = land_peak_bucket-1; i >= 0; i--){
		int bucket_count = histogram.getCountForBucket(i);

	   if(bucket_count > histogram.getCountForBucket(i+1)){
		   increasing_buckets += 1;

		  if(increasing_buckets >= minimum_increasing_bins_for_rising_trend)
			   break;
	   }
	   else{
		   increasing_buckets = std::max(increasing_buckets-1, 0);

		   if(bucket_count < histogram.getCountForBucket(minimum_between_land_and_cloud_peak_bucket))
			   minimum_between_land_and_cloud_peak_bucket = i;
	   }
	}
	double minimum_between_land_and_cloud_peak_value = histogram.calculateBucketLowerBorder(minimum_between_land_and_cloud_peak_bucket);
	histogram.addMarker(minimum_between_land_and_cloud_peak_value, "minimum: "+std::to_string(minimum_between_land_and_cloud_peak_value)+" bucket: "+std::to_string(minimum_between_land_and_cloud_peak_bucket));


	/**now we know the bucket of the cloud threshold :) **/

	return minimum_between_land_and_cloud_peak_value;//minimum_between_land_and_cloud_peak_bucket;
}

template<typename T1, typename T2>
struct PlsNoDontDoThis{
	static void execute(Raster2D<T1> *sza_raster, Raster2D<T2> *out_raster, std::vector<float> classification_thresholds, std::vector<float> classification_classes, int number_of_classes) {

		int width = out_raster->width;
		int height = out_raster->height;

		for(int i = 0; i < width; i++){
			for (int j = 0; j < height; j++){
		//start with NAN
			T2 outputValue = out_raster->dd.no_data;
			T1 inputValue = sza_raster->getSafe(i,j);

				for(int k=0; k<number_of_classes; k++) {
					float lowerThreshold = classification_thresholds[2*k];
					float upperThreshold = classification_thresholds[2*k+1];

					if( (inputValue >= lowerThreshold) && (inputValue <= upperThreshold) ){
						outputValue = classification_classes[k];
					}

					//printf("OCL gid = %d, inputValue = %f, temp = %f, lowerThreshold = %f, upperThreshold = %f , outputValue= %f \n", gid, inputValue, outputValue, lowerThreshold, upperThreshold, outputValue);
				}

			out_raster->setSafe(i,j,outputValue);

			}
		}

	}
};


MSATGccThermThresholdDetectionOperator::MSATGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MSATGccThermThresholdDetectionOperator::~MSATGccThermThresholdDetectionOperator() {
}
REGISTER_OPERATOR(MSATGccThermThresholdDetectionOperator, "msatgccthermthresholddetection");



std::unique_ptr<GenericRaster>  MSATGccThermThresholdDetectionOperator::getRaster(const QueryRectangle &rect, QueryProfiler &profiler) {
	std::ofstream logfile( "/tmp/loggcctherm.txt" );


	auto solar_zenith_angle_raster = getRasterFromSource(0, rect, profiler);
	auto bt108_minus_bt039_raster = getRasterFromSource(1, rect, profiler);

	Profiler::Profiler p("MSATGCCTHERMTHRESHOLDDETECTION_OPERATOR");
	solar_zenith_angle_raster->setRepresentation(GenericRaster::CPU);
	bt108_minus_bt039_raster->setRepresentation(GenericRaster::CPU);

	double bucket_size = 1.0/3.0;
	int increasing_buckets_for_rising_trend = 4;
	int minimum_values_in_histogram = 500;
	double minimum_land_peak_temperature = -1;
	double temperature_threshold_night, temperature_threshold_day = -1;

	logfile<<"bucket_size: "<<bucket_size<<"|increasing_buckets_for_rising_trend: "<<increasing_buckets_for_rising_trend<<"|minimum_land_peak_temperature: "<<minimum_land_peak_temperature<<std::endl;

	logfile<<"DAY"<<std::endl;
	auto histogram_day_ptr = callBinaryOperatorFunc<CreateConditionalHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), bucket_size, cloudclass::solar_zenith_angle_min_day, cloudclass::solar_zenith_angle_max_day);
	logfile<<histogram_day_ptr->toJSON()<<std::endl;
	if(histogram_day_ptr->getValidDataCount()>minimum_values_in_histogram)
		temperature_threshold_day = findGccThermThreshold(*histogram_day_ptr.get(), minimum_land_peak_temperature, increasing_buckets_for_rising_trend);
	logfile<<"temperature_threshold_day: "<<temperature_threshold_day<<std::endl;
	auto histogram_night_ptr = callBinaryOperatorFunc<CreateConditionalHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), bucket_size, cloudclass::solar_zenith_angle_min_night, cloudclass::solar_zenith_angle_max_night);

	logfile<<"NIGHT"<<std::endl;
	logfile<<histogram_night_ptr->toJSON()<<std::endl;
	if(histogram_night_ptr->getValidDataCount()>minimum_values_in_histogram)
		temperature_threshold_night = findGccThermThreshold(*histogram_night_ptr.get(), minimum_land_peak_temperature, increasing_buckets_for_rising_trend);
	logfile<<"temperature_threshold_night: "<<temperature_threshold_night<<std::endl;

	//build vectors
	std::vector<float> thresholds{static_cast<float>(cloudclass::solar_zenith_angle_min_day), static_cast<float>(cloudclass::solar_zenith_angle_max_day), static_cast<float>(cloudclass::solar_zenith_angle_min_night), static_cast<float>(cloudclass::solar_zenith_angle_max_night)};
	std::vector<float> classes{static_cast<float>(temperature_threshold_day), static_cast<float>(temperature_threshold_night)};

	//return std::unique_ptr<GenericPlot>(std::move(histogram_night_ptr));
	std::cerr<<"temperature_threshold_day: "<<temperature_threshold_day<<" |temperature_threshold_night: "<<temperature_threshold_night<<" |thresholds: "<<thresholds.at(3)<<" |classes:"<<classes.at(1)<<std::endl;

	//solar_zenith_angle_raster->setRepresentation(GenericRaster::Representation::OPENCL);

	DataDescription out_dd(GDT_Float32, std::min(temperature_threshold_day, temperature_threshold_night), std::max(temperature_threshold_day, temperature_threshold_night)); // no no_data //raster->dd.has_no_data, output_no_data);
	out_dd.addNoData();
	auto raster_out = GenericRaster::create(out_dd, *solar_zenith_angle_raster, GenericRaster::Representation::CPU);

	//RasterOpenCL::CLProgram prog;
	//	prog.setProfiler(profiler);
	//	prog.addOutRaster(raster_out.get());
	//	prog.addInRaster(solar_zenith_angle_raster.get());
	//	prog.compile(operators_msat_classification_kernels, "valueClassificationKernel");
	//	prog.addArg(thresholds);
	//	prog.addArg(classes);
	//	prog.addArg(2);
	//	prog.run();


	callBinaryOperatorFunc<PlsNoDontDoThis>(solar_zenith_angle_raster.get(), raster_out.get(), thresholds, classes, 2);
	return raster_out;
}
