#ifndef DATATYPES_MULTILINECOLLECTION_H_
#define DATATYPES_MULTILINECOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"

/**
 * This collection contains Multi-Lines
 */
class MultiLineCollection : public SimpleFeatureCollection {
public:

	MultiLineCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref) {
		start_feature.push_back(0); //end of first feature
		start_line.push_back(0); //end of first line
	}

	//starting index of individual lines in the points vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over lines has to stop at start_line.size() -2
	std::vector<uint32_t> start_line;

	//starting index of individual features in the startElement vector, last entry indicates first index out of bounds of start_line
	//thus iterating over features has to stop at start_feature.size() -2
	std::vector<uint32_t> start_feature;

	virtual std::string toGeoJSON(bool displayMetadata) const;
	virtual std::string toCSV() const;

	virtual bool isSimple() const;

	virtual size_t getFeatureCount() const {
		return start_feature.size() - 1;
	}

	virtual ~MultiLineCollection(){};
};

#endif
