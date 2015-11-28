/*
 * sizeutil.cpp
 *
 *  Created on: 28.11.2015
 *      Author: mika
 */

#include "sizeutil.h"
#include "datatypes/plot.h"
#include "datatypes/simplefeaturecollection.h"

size_t sizeutil::helper<GenericPlot>::get_size( const GenericPlot &value) {
	(void) value;
	return 1024 * 10; // 10 KiB
}

size_t sizeutil::helper<std::string>::get_size( const std::string &value) {
	return sizeof(std::string) + value.length();
}

size_t sizeutil::helper<std::vector<Coordinate>>::get_size( const std::vector<Coordinate> &value) {
	return 24 + value.size() * sizeof(Coordinate);
}
