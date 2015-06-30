
#include "util/exceptions.h"
#include "datatypes/plots/histogram.h"

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

double Histogram::calculateBucketLowerBorder(int bucket){
	return (bucket * ((max - min) / counts.size())) + min;
}

void Histogram::incNoData() {
	nodata_count++;
}

int Histogram::getValidDataCount(){
	//return std::accumulate(counts.begin(), counts.end(), 0);
	int sum = 0;
	for (int &i: counts)
	  {
	    sum += i;
	  }
	return sum;
}

void Histogram::addMarker(double bucket, const std::string &label){
	markers.emplace_back(bucket, label);
}

std::string Histogram::toJSON() {
	std::stringstream buffer;
	buffer << "{\"type\": \"histogram\", ";
	buffer << "\"metadata\": {\"min\": " << min << ", \"max\": " << max << ", \"nodata\": " << nodata_count << ", \"numberOfBuckets\": " << counts.size() << "}, ";
	buffer << "\"data\": [";
	for(size_t i=0; i < counts.size(); i++){
				if(i != 0)
					buffer <<" ,";
		buffer << counts.at(i);
	}
	buffer << "] ";
	if(markers.size() > 0){
		buffer << ", ";
		buffer <<"\"lines\":[";

		for(size_t i=0; i < markers.size(); i++){
			if(i != 0)
				buffer <<" ,";
			auto marker = markers.at(i);
			buffer <<"{\"name\":\"" << marker.second << "\" ,\"pos\":" << std::to_string(marker.first) << "}";
		}
		buffer << "]";
	}
	buffer << "} ";
	return buffer.str();
}
