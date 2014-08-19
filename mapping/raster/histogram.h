#ifndef RASTER_HISTOGRAM_H
#define RASTER_HISTOGRAM_H

#include <vector>
#include <sstream>
#include <string>

#include "raster/datavector.h"

class Histogram : public DataVector {
	public:
		Histogram(int number_of_buckets, double min, double max);
		~Histogram();

		void inc(double value);
		void incNoData();

		std::string toJSON();

		static const int DEFAULT_NUMBER_OF_BUCKETS = 10000;

	private:
		std::vector<int> counts;
		int nodata_count;
		double min, max;
};

#endif
