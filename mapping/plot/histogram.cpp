
#include "raster/exceptions.h"
#include "plot/histogram.h"

#include <cstdio>
#include <cmath>
#include <limits>


Histogram::Histogram(int number_of_buckets, double min, double max)
	: counts(number_of_buckets), nodata_count(0), min(min), max(max) {

	if (!std::isfinite(min) || !std::isfinite(max))
		throw ArgumentException("Histogram: min or max not finite");
	if (min >= max)
		throw ArgumentException("Histogram: min >= max");
}


Histogram::~Histogram() {

}


void Histogram::inc(double value) {
	if (value < min || value > max) {
		incNoData();
		return;
	}

	//int bucket = std::floor(((value - min) / (max - min)) * counts.size());
	//bucket = std::min((int) counts.size()-1, std::max(0, bucket));
	//counts[bucket]++;
	counts[calculateBucketForValue(value)]++;
}

int Histogram::calculateBucketForValue(double value){
	int bucket = std::floor(((value - min) / (max - min)) * counts.size());
	bucket = std::min((int) counts.size()-1, std::max(0, bucket));
	return bucket;
}


void Histogram::incNoData() {
	nodata_count++;
}

std::string Histogram::toJSON() {
	std::stringstream buffer;
	buffer << "{\"type\": \"histogram\", ";
	buffer << "\"metadata\": {\"min\": " << min << ", \"max\": " << max << ", \"nodata\": " << nodata_count << ", \"numberOfBuckets\": " << counts.size() << "}, ";
	buffer << "\"data\": [";
	for(int& bucket : counts) {
		buffer << bucket << ",";
	}
	buffer.seekp(((long) buffer.tellp()) - 1);
	buffer << "]}";
	return buffer.str();
}
