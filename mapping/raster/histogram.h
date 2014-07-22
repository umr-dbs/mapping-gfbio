#ifndef RASTER_HISTOGRAM_H
#define RASTER_HISTOGRAM_H

#include <vector>

class Histogram {
	private:
		int nodata_count;
		std::vector<int> counts;
		double min, max;
		double bucketSize;
	public:
		Histogram(int n, double min, double max) : counts(n), nodata_count(0), min(min), max(max), bucketSize((max-min)/n) { }
		~Histogram() {}
		void print();
		void add(double value);
		void addNoDataEntry();
};

#endif
