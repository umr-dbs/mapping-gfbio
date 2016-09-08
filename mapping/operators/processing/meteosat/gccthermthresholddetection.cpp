
#include "datatypes/raster.h"
#include "datatypes/raster/typejuggling.h"
#include "raster/profiler.h"
#include "raster/opencl.h"
#include "operators/operator.h"
#include "msg_constants.h"
#include "sofos_constants.h"
#include "datatypes/plots/histogram.h"

#include <memory>
#include <math.h>
//#include <time.h>
//#include <iterator>
#include <json/json.h>

class MeteosatGccThermThresholdDetectionOperator : public GenericOperator {
	public:
		MeteosatGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &/*params*/);
		virtual ~MeteosatGccThermThresholdDetectionOperator();

#ifndef MAPPING_OPERATOR_STUBS
		virtual std::unique_ptr<GenericRaster> getRaster(const QueryRectangle &rect, const QueryTools &tools);
		virtual std::unique_ptr<GenericPlot> getPlot(const QueryRectangle &rect, const QueryTools &tools);
#endif

	private:
#ifndef MAPPING_OPERATOR_STUBS
		double findGccThermThreshold(Histogram *histogram);
#endif
		const double bucket_size{1.0/2.0}; //this is the bucket size used for the histogram(s) //default: 1/3 -> this is problematic as the BBT is now calculated using the Eumetsat LUT with steps like .0 .5 .0 .5 .0 .5 ...
		const int minimum_increasing_buckets_for_rising_trend{3}; //threshold detection phase 1 uses this to determine the minimum between cloud and land peak //default:3
		const int minimum_soft_falling_buckets{3}; //threshold detection phase 2 uses this to detect a cloud peak merged into the land peak //default:2
		const double minimum_land_peak_temperature{-5}; //this is the minimum land peak temperature. //default:-1
		const double minimum_cloud_threshold_temperature{-12}; //this is the minimum valid cloud threshold temperature //default:-12
		const double cloud_minimum_and_peak_ratio{0.85}; //ratio to determine if the minimum between land and cloud peak is ?distinct?
		const double merged_peaks_bucket_ratio_bound_lower{0.8}; //default is 0.8
		const double merged_peaks_bucket_ratio_bound_higher{1.00}; //default is 1
};


#include "operators/processing/raster/classification_kernels.cl.h"

MeteosatGccThermThresholdDetectionOperator::MeteosatGccThermThresholdDetectionOperator(int sourcecounts[], GenericOperator *sources[], Json::Value &params) : GenericOperator(sourcecounts, sources) {
	assumeSources(1);
}
MeteosatGccThermThresholdDetectionOperator::~MeteosatGccThermThresholdDetectionOperator() {
}
REGISTER_OPERATOR(MeteosatGccThermThresholdDetectionOperator, "meteosat_gccthermthresholddetection");


#ifndef MAPPING_OPERATOR_STUBS
#ifdef MAPPING_NO_OPENCL
std::unique_ptr<GenericRaster> MeteosatGccThermThresholdDetectionOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	throw OperatorException("MSATGccThermThresholdDetectionOperator: cannot be executed without OpenCL support");
}
std::unique_ptr<GenericPlot> MeteosatGccThermThresholdDetectionOperator::getPlot(const QueryRectangle &rect, const QueryTools &tools) {
	throw OperatorException("MSATGccThermThresholdDetectionOperator: cannot be executed without OpenCL support");
}
#else

template<typename T1, typename T2>
struct RasterClassification{
	static void execute(Raster2D<T1> *sza_raster, Raster2D<T2> *out_raster, std::vector<float> classification_bounds_lower, std::vector<float> classification_bounds_upper, std::vector<float> classification_classes) {

		int width = out_raster->width;
		int height = out_raster->height;

		for(int i = 0; i < width; i++){
			for (int j = 0; j < height; j++){

			T2 outputValue = out_raster->dd.no_data; //start with no data
			T1 inputValue = sza_raster->getSafe(i,j);

				for(size_t k=0; k<classification_classes.size(); k++) {
					float lowerThreshold = classification_bounds_lower[k];
					float upperThreshold = classification_bounds_upper[k];

					if( (inputValue >= lowerThreshold) && (inputValue <= upperThreshold) ){
						outputValue = classification_classes[k];
					}
				}
			out_raster->setSafe(i,j,outputValue);
			}
		}
	}
};


template<typename T1, typename T2>
struct ConditionalFillHistogramFunction{
	static void execute(Raster2D<T1> *value_raster, Raster2D<T2> *condition_raster, Histogram *histogram, double condition_min, double condition_max) {
		value_raster->setRepresentation(GenericRaster::Representation::CPU);
		condition_raster->setRepresentation(GenericRaster::Representation::CPU);

		int size_a = value_raster->getPixelCount();
		int size_b = condition_raster->getPixelCount();
		if(size_a != size_b){
			//do something here
		}

	   for (int i=0;i<size_a;i++) {
		   T1 value = value_raster->data[i];
		   T2 condition_value = condition_raster->data[i];
		   if (value_raster->dd.is_no_data(value) || condition_raster->dd.is_no_data(condition_value)){
			   histogram->incNoData();
		   }
		   else if(condition_value >= condition_min && condition_value < condition_max) {
			   histogram->inc(value);
		   }
		   else{
			   histogram->incNoData();
		   }
	   }

	   return;
	}
};

/**
 * This function determines the bucket of the land_peak
 */
int phase1FindLandPeakBucket(Histogram *histogram, const double minimum_land_peak_temperature){

	//set the land_peak_bucket to the bucket containing the minimum_land_peak_temperature
	int land_peak_bucket = histogram->calculateBucketForValue(minimum_land_peak_temperature);

	//if there are buckets above the land_peak_bucket calculated for the minimum_land_peak_temperature -> search the bucket with max. count. This is the real land peak.
	if(histogram->getMax() > minimum_land_peak_temperature){
		int land_peak_count = histogram->getCountForBucket(land_peak_bucket);
		for(int i = land_peak_bucket; i < histogram->getNumberOfBuckets(); i++){
			int tempCount = histogram->getCountForBucket(i);
			if(tempCount > land_peak_count){
				land_peak_count = tempCount;
				land_peak_bucket = i;
			}
		}
	}
	return (land_peak_bucket);
}

/**
 * This function determines the bucket of the minimum between a land and cloud peak (and the number of increasing buckets):
 * The minimum is indicated by at least 'minimum_increasing_buckets_for_rising_trend' buckets with greater values on the cloud peak side.
 */
std::pair<int,int> phase1FindMinimumWithRisingTrendBetweenLandAndCloudPeak(Histogram *histogram, const double minimum_increasing_buckets_for_rising_trend, const int land_peak_bucket){

	int minimum_between_land_and_cloud_peak_bucket = land_peak_bucket;
	int increasing_buckets = 0;
	/** starting from land peak bucket we are searching buckets at lower positions with rising counts.
	 ** TODO: this approach is strange as it allows a pattern like 10 11 12 -10 9 8 7 with minimum_increasing_buckets_for_rising_trend = 4.
	 ** 	One solution would be: if there is a new minimum -> increasing_buckets should be reset to 0....
	 **/
	for(int i = land_peak_bucket-1; i >= 0; i--){
		int bucket_count = histogram->getCountForBucket(i);
		if(bucket_count > histogram->getCountForBucket(i+1)){
			increasing_buckets += 1;
			if(increasing_buckets >= minimum_increasing_buckets_for_rising_trend)
				break;
		}
		else{
			increasing_buckets = std::max(increasing_buckets-1, 0);
			if(bucket_count < histogram->getCountForBucket(minimum_between_land_and_cloud_peak_bucket))
				 minimum_between_land_and_cloud_peak_bucket = i;
		}
	}
	return (std::pair<int,int>{minimum_between_land_and_cloud_peak_bucket, increasing_buckets});
}

/**
 * This function determines the cloud peak at a position <(=) minimum_between_land_and_cloud_peak_bucket
 */
int phase1FindCloudPeakBucket(Histogram *histogram, const int minimum_between_land_and_cloud_peak_bucket){
	//start at the minimum between bucket
	int cloud_peak_bucket = minimum_between_land_and_cloud_peak_bucket;
	//now simply find the maximum
	for(int i = minimum_between_land_and_cloud_peak_bucket; i >= 0; i--){
		if(histogram->getCountForBucket(i) > histogram->getCountForBucket(minimum_between_land_and_cloud_peak_bucket)){
			cloud_peak_bucket = i;
		}
	}
	return (cloud_peak_bucket);
}

/**
 * This function detects a cloud peak merged into a land peak
 */
int phase2FindMergedPeaksBucket(Histogram *histogram, const int minimum_soft_falling_buckets, const int land_peak_bucket, const double bucket_ratio_bound_lower,  const double bucket_ratio_bound_higher){
	int merged_peeks_soft_falling_bucket = 0;
	int soft_falling_buckets = 0;
	for(int i = land_peak_bucket-1; i >= 0; i--){
		double bucket_ratio = static_cast<double>(histogram->getCountForBucket(i)) / static_cast<double>(histogram->getCountForBucket(i+1));
			if((bucket_ratio > bucket_ratio_bound_lower) & (bucket_ratio <=bucket_ratio_bound_higher)){
				soft_falling_buckets += 1;
			if(soft_falling_buckets >= minimum_soft_falling_buckets){
				merged_peeks_soft_falling_bucket = i + soft_falling_buckets; //go back to the start of the matching buckets
				break;
			}
		}
		else{
			soft_falling_buckets = 0;
		}
	}
	return (merged_peeks_soft_falling_bucket);
}

/**
 * This function detects a dynamic threshold for seperating cloudy pixels from land (or sea) pixels based on IR10.8-IR03.9 and the solar zenith angle of a meteosat scene based on:
 * Cermak, Jan. SOFOS - A New Satellite-based Operational Fog Observation Scheme. http://archiv.ub.uni-marburg.de/diss/z2006/0149. Philipps-Universit�t Marburg, 2006. [Page 45ff].
 * Please note: The implementation is based on the original FORTRAN sources which includes numerous changes/adaptations not reflected in the publication.
 * Additionally note: SOFOS was developed for a region that resembles the BBOX of Europe. Validation for other regions or region sizes other than 767px*510px is still needed!
 */
double MeteosatGccThermThresholdDetectionOperator::findGccThermThreshold(Histogram *histogram/*, const double minimum_land_peak_temperature, const double minimum_cloud_threshold_temperature, const int minimum_increasing_bins_for_rising_trend, const int minimum_soft_falling_buckets */){

	/** PHASE 1 **/

	/**phase 1: The first step is to find the bucket of the land peak in the histogram**/
	int land_peak_bucket = phase1FindLandPeakBucket(histogram, this-> minimum_land_peak_temperature);

	/**phase 1: If there is a clear minimum between the land peak and the (still to find) cloud peak this is the cloud threshold.
	 **			The minimum is indicated by 'rising trend of minimum_increasing_buckets_for_rising_trend' buckets.
	 **/
	std::pair<int,int> minimum_between_and_increasing_buckets = phase1FindMinimumWithRisingTrendBetweenLandAndCloudPeak(histogram, this->minimum_increasing_buckets_for_rising_trend, land_peak_bucket);
	int minimum_between_land_and_cloud_peak_bucket = minimum_between_and_increasing_buckets.first;
	int increasing_buckets = minimum_between_and_increasing_buckets.second; //TODO remove this and change return value to int

	/** new we need to find the cloud peek which should be the maximum bucket below the minimum_between_land_and_cloud_peak_bucket**/
	int cloud_peak_bucket = phase1FindCloudPeakBucket(histogram, minimum_between_land_and_cloud_peak_bucket);

	/** PHASE 2 **/

	/** phase2: if there are no clear differentiated peeks we must use a different method to identify the cloud threshold.
	 ** 		Find the point where the falling slope of the land peak decreases significantly
	 **/
	int merged_peeks_soft_falling_bucket = phase2FindMergedPeaksBucket(histogram, this->minimum_soft_falling_buckets, land_peak_bucket, this->merged_peaks_bucket_ratio_bound_lower, this->merged_peaks_bucket_ratio_bound_higher);

	/** now we know the buckets of the peaks and the minimum and can get the values **/
	double land_peak_value = histogram->calculateBucketLowerBorder(land_peak_bucket);
	double minimum_between_land_and_cloud_peak_value = histogram->calculateBucketLowerBorder(minimum_between_land_and_cloud_peak_bucket);
	double cloud_peak_value = histogram->calculateBucketLowerBorder(cloud_peak_bucket);
	double merged_peeks_soft_falling_value = histogram->calculateBucketLowerBorder(merged_peeks_soft_falling_bucket);

	/** check if the minimum is a valid cloud threshold **/ //TODO: refactor as function
	bool cant_use_minimum_as_cloud_threshold =	(increasing_buckets < minimum_increasing_buckets_for_rising_trend) | // rising trends was not met TODO: this is always covered by the next check and could be removed (if the histogram is not to small)
											(minimum_between_land_and_cloud_peak_value < minimum_cloud_threshold_temperature) | // cloud_threshold is to cold
											((cloud_peak_bucket != minimum_between_land_and_cloud_peak_bucket) & ((minimum_between_land_and_cloud_peak_value/cloud_peak_value)>cloud_minimum_and_peak_ratio)); //exp: cloud area is to flat

	/**add annotations for peaks and minimum to the histogram**/
	histogram->addMarker(land_peak_value, "landpeak: "+std::to_string(land_peak_value)+" bucket: "+std::to_string(land_peak_bucket));
	histogram->addMarker(minimum_between_land_and_cloud_peak_value, "minimum: "+std::to_string(minimum_between_land_and_cloud_peak_value)+" bucket: "+std::to_string(minimum_between_land_and_cloud_peak_bucket)+" cant use: "+std::to_string(cant_use_minimum_as_cloud_threshold));
	histogram->addMarker(cloud_peak_value, "cloudpeak: "+std::to_string(cloud_peak_value)+" bucket: "+std::to_string(cloud_peak_bucket));
	histogram->addMarker(merged_peeks_soft_falling_value, "mergedpeeks soft falling: "+std::to_string(merged_peeks_soft_falling_value)+" bucket: "+std::to_string(merged_peeks_soft_falling_bucket));

	/** now decide if we should use the minimum between peeks or the merged peeks value**/
	if(cant_use_minimum_as_cloud_threshold){
		return (merged_peeks_soft_falling_value); //return merged peeks value
	}else{
		return (minimum_between_land_and_cloud_peak_value); //return minimum between peeks value
	}
}

std::unique_ptr<GenericRaster> MeteosatGccThermThresholdDetectionOperator::getRaster(const QueryRectangle &rect, const QueryTools &tools) {
	//get the input rasters
	auto solar_zenith_angle_raster = getRasterFromSource(0, rect, tools);
	auto bt108_minus_bt039_raster = getRasterFromSource(1, rect, tools);

	// TODO: verify units of the source rasters
	if (!bt108_minus_bt039_raster->dd.unit.hasMinMax())
		throw OperatorException("source raster does not have a proper unit");

	//setup the profiler
	Profiler::Profiler p("MSATGCCTHERMTHRESHOLDDETECTION_OPERATOR");

	//move rasters to cpu if needed
	solar_zenith_angle_raster->setRepresentation(GenericRaster::CPU);
	bt108_minus_bt039_raster->setRepresentation(GenericRaster::CPU);

	//get min and max values and calculate the needed buckets
	double value_raster_min = bt108_minus_bt039_raster->dd.unit.getMin();
	double value_raster_max = bt108_minus_bt039_raster->dd.unit.getMax();
	int buckets = static_cast<int>(std::ceil((value_raster_max-value_raster_min)/bucket_size));

	//create the histogram for day mode
	std::unique_ptr<Histogram> histogram_day_ptr = make_unique<Histogram>(buckets, value_raster_min, value_raster_max);
	//fill the histogram
	callBinaryOperatorFunc<ConditionalFillHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), histogram_day_ptr.get(), cloudclass::solar_zenith_angle_min_day, cloudclass::solar_zenith_angle_max_day);
	//get the threshold
	double temperature_threshold_day = findGccThermThreshold(histogram_day_ptr.get());

	//create the histogram for day mode
	std::unique_ptr<Histogram> histogram_night_ptr = make_unique<Histogram>(buckets, value_raster_min, value_raster_max);
	//fill the histogram
	callBinaryOperatorFunc<ConditionalFillHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), histogram_night_ptr.get(), cloudclass::solar_zenith_angle_min_night, cloudclass::solar_zenith_angle_max_night);
	//get the threshold
	double temperature_threshold_night = findGccThermThreshold(histogram_night_ptr.get());

	//build vectors for classification -> The class in the middle sets a threshold of -9999 which will never happen in reality.  This is needed to prevent filling "twilight" areas with NaN.
	std::vector<float> classification_bounds_lower{static_cast<float>(cloudclass::solar_zenith_angle_min_day), static_cast<float>(cloudclass::solar_zenith_angle_max_day), static_cast<float>(cloudclass::solar_zenith_angle_min_night)};
	std::vector<float> classification_bounds_upper{static_cast<float>(cloudclass::solar_zenith_angle_max_day), static_cast<float>(cloudclass::solar_zenith_angle_min_night), static_cast<float>(cloudclass::solar_zenith_angle_max_night)};
	std::vector<float> classification_classes{static_cast<float>(temperature_threshold_day), -9999 , static_cast<float>(temperature_threshold_night)};

	//move the zenith_angle_raster to the device
	solar_zenith_angle_raster->setRepresentation(GenericRaster::Representation::CPU); //TODO: do GPU

	//create the output raster
	double min = std::min(temperature_threshold_day, temperature_threshold_night);
	double max = std::max(temperature_threshold_day, temperature_threshold_night);
	Unit out_unit("unknown", "unknown"); // TODO: proper unit
	out_unit.setMinMax(min, max);
	DataDescription out_dd(GDT_Float32, out_unit); // no no_data //raster->dd.has_no_data, output_no_data);
	out_dd.addNoData();
	auto raster_out = GenericRaster::create(out_dd, *solar_zenith_angle_raster, GenericRaster::Representation::OPENCL);

	//Use the OpenCL classification kernel to fill a raster with the values
	RasterOpenCL::CLProgram prog;
	prog.setProfiler(tools.profiler);
	prog.addOutRaster(raster_out.get());
	prog.addInRaster(solar_zenith_angle_raster.get());
	prog.compile(operators_processing_raster_classification_kernels, "replacementByRangeKernel");
	prog.addArg(classification_bounds_lower);
	prog.addArg(classification_bounds_upper);
	prog.addArg(classification_classes);
	prog.addArg(static_cast<int>(classification_classes.size()));
	prog.addArg(static_cast<float>(out_dd.no_data)); //keep nodata as nodata
	prog.run();


	//callBinaryOperatorFunc<RasterClassification>(solar_zenith_angle_raster.get(), raster_out.get(), classification_bounds_lower, classification_bounds_upper, classification_classes);
	return (raster_out);
}

std::unique_ptr<GenericPlot> MeteosatGccThermThresholdDetectionOperator::getPlot(const QueryRectangle &rect, const QueryTools &tools) {
	//get the input rasters
	auto solar_zenith_angle_raster = getRasterFromSource(0, rect, tools);
	auto bt108_minus_bt039_raster = getRasterFromSource(1, rect, tools);

	// TODO: verify units of the source rasters
	if (!bt108_minus_bt039_raster->dd.unit.hasMinMax())
		throw OperatorException("source raster does not have a proper unit");

	//setup the profiler
	Profiler::Profiler p("MSATGCCTHERMTHRESHOLDDETECTION_OPERATOR");

	//move rasters to cpu if needed
	solar_zenith_angle_raster->setRepresentation(GenericRaster::CPU);
	bt108_minus_bt039_raster->setRepresentation(GenericRaster::CPU);

	//get min and max values and calculate the needed buckets
	double value_raster_min = bt108_minus_bt039_raster->dd.unit.getMin();
	double value_raster_max = bt108_minus_bt039_raster->dd.unit.getMax();
	int buckets = static_cast<int>(std::ceil((value_raster_max-value_raster_min)/bucket_size));

	//create the histogram
	std::unique_ptr<Histogram> histogram_ptr = make_unique<Histogram>(buckets, value_raster_min, value_raster_max);
	//fill the histogram
	callBinaryOperatorFunc<ConditionalFillHistogramFunction>(bt108_minus_bt039_raster.get(), solar_zenith_angle_raster.get(), histogram_ptr.get(), cloudclass::solar_zenith_angle_min_day, cloudclass::solar_zenith_angle_max_day);
	//find the GccThermalThreshold and annotate the histogram
	findGccThermThreshold(histogram_ptr.get());

	//return the histogram as plot
	return (std::unique_ptr<GenericPlot>(std::move(histogram_ptr)));;
}
#endif
#endif
