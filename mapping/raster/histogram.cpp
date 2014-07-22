
#include "raster/histogram.h"

#include <cstdio>


void Histogram::print() {
	printf("{\"min\": %f, \"max\": %f, \"nodata\": %d, \"buckets\": [", min, max, nodata_count);
	for (auto it = counts.begin(); it != counts.end(); it++) {
		if (it != counts.begin())
			printf(",");
		printf("%d", *it);
	}
	printf("]}");
}


void Histogram::add(double value) {
	if (value >= min && value < max) {
		unsigned int bucket = (value - min) / bucketSize;

		// prevent overflows
		if (bucket >= counts.size())
			bucket = counts.size() - 1;
		//if (bucket < 0)
		//	bucket = 0;

		counts[bucket]++;
	}
}

void Histogram::addNoDataEntry() {
	nodata_count++;
}
