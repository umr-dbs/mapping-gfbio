#ifndef DATATYPES_MULTIPOINTCOLLECTION_H_
#define DATATYPES_MULTIPOINTCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"
#include <memory>

/**
 * This collection contains Multi-Points
 */
class MultiPointCollection : public SimpleFeatureCollection {
public:
	MultiPointCollection(BinaryStream &stream);
	using SimpleFeatureCollection::SimpleFeatureCollection; //inherit constructor

	//starting index of individual features in the points vector
	std::vector<uint32_t> startFeature;

	void toStream(BinaryStream &stream);

	// add a new point, returns index of the new point
	size_t addPoint(double x, double y);

	std::unique_ptr<MultiPointCollection> filter(const std::vector<bool> &keep);
	std::unique_ptr<MultiPointCollection> filter(const std::vector<char> &keep);

	std::string hash();

	virtual std::string toGeoJSON(bool displayMetadata);
	virtual std::string toCSV();

	virtual ~MultiPointCollection(){};
};

#endif
