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
	std::vector<uint32_t> start_feature;

	void toStream(BinaryStream &stream);

	// add a new coordinate,
	void addCoordinate(double x, double y);
	//add a new feature consisting of given coordinates, returns new feature index
	size_t addFeature(const std::vector<Coordinate> &coordinates);
	//add a new feature consisting of a single coordinate, returns new feature index
	size_t addFeature(const Coordinate coordinate);

	std::unique_ptr<MultiPointCollection> filter(const std::vector<bool> &keep);
	std::unique_ptr<MultiPointCollection> filter(const std::vector<char> &keep);

	std::string hash();

	virtual std::string toGeoJSON(bool displayMetadata);
	virtual std::string toCSV();

	virtual bool isSimple();

	//return the index of the next feature in the startPolygon array that is no longer part of the index-th feature
	inline size_t stopFeature(size_t index) const {
		return index + 1 >= start_feature.size() ? coordinates.size() : start_feature[index + 1];
	}

	virtual ~MultiPointCollection(){};
};

#endif
