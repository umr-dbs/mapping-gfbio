
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
