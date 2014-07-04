#ifndef RASTER_HISTOGRAM_H
#define RASTER_HISTOGRAM_H

#include <vector>

class Histogram {
	public:
		Histogram(int n, double min, double max) : counts(n), nodata_count(0), min(min), max(max) {}
		~Histogram() {}
		void print();
		std::vector<int> counts;
		int nodata_count;
		double min, max;
};

#endif
