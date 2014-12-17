#ifndef PLOT_HISTOGRAM_H
#define PLOT_HISTOGRAM_H

#include <vector>
#include <sstream>
#include <string>

#include "plot/plot.h"

class Histogram : public GenericPlot {
	public:
		Histogram(int number_of_buckets, double min, double max);
		virtual ~Histogram();

		void inc(double value);
		void incNoData();

		std::string toJSON();

		static const int DEFAULT_NUMBER_OF_BUCKETS = 10000;

		int getCountForBucket(int bucket){
			return counts.at(bucket);
		}
		int getNoDataCount(){
			return nodata_count;
		}
		double getMin(){
			return min;
		}
		double getMax(){
			return max;
		}
		int getNumberOfBuckets(){
			return counts.size();
		}
		/*
		 * returns the count of all inserted elements (without NoData)
		 */
		int getValidDataCount();
		/**
		 * calculates the bucket where a value would be inserted
		 */
		int calculateBucketForValue(double value);
		/**
		 * calculates the bucket minimum
		 */
		double calculateBucketLowerBorder(int bucket);
		/**
		 * add a marker
		 */
		void addMarker(double bucket, std::string label){
			markers.emplace_back(bucket, label);
		}

	private:
		std::vector<int> counts;
		int nodata_count;
		double min, max;

		std::vector<std::pair<double, std::string>> markers;
};

#endif
