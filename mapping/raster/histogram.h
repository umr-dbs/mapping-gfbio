#ifndef RASTER_HISTOGRAM_H
#define RASTER_HISTOGRAM_H

#include <vector>

class Histogram {
	public:
		Histogram(int number_of_buckets, double min, double max);
		~Histogram();
		void print();
		void inc(double value);
		void incNoData();

		static const int DEFAULT_NUMBER_OF_BUCKETS = 10000;

	private:
		std::vector<int> counts;
		int nodata_count;
		double min, max;
};

#endif
