#ifndef PLOT_HISTOGRAM_H
#define PLOT_HISTOGRAM_H

#include <vector>
#include <sstream>
#include <string>

#include "plot/plot.h"

class Histogram : public GenericPlot {
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
