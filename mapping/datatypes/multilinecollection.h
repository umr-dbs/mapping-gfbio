#ifndef DATATYPES_MULTILINECOLLECTION_H_
#define DATATYPES_MULTILINECOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"

/**
 * This collection contains Multi-Lines
 */
class MultiLineCollection : public SimpleFeatureCollection {
public:

	using SimpleFeatureCollection::SimpleFeatureCollection; //inherit constructor

	//starting index of individual lines in the points vector
	std::vector<uint32_t> startLine;

	//starting index of individual features in the startElement vector
	std::vector<uint32_t> startFeature;

	virtual std::string toGeoJSON(bool displayMetadata);
	virtual std::string toCSV();

	virtual bool isSimple();

	virtual ~MultiLineCollection(){};
};

#endif
