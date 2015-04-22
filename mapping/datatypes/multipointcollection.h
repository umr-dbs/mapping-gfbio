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
	MultiPointCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref) {
		start_feature.push_back(0); //end of first feature
	}

	//starting index of individual features in the points vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over features has to stop at start_feature.size() -2
	std::vector<uint32_t> start_feature;

	void toStream(BinaryStream &stream);

	//add a new coordinate, to a new feature. After adding all coordinates, finishFeature() has to be called
	void addCoordinate(double x, double y);
	//finishes the definition of the new feature, returns new feature index
	size_t finishFeature();
	//add a new feature consisting of a single coordinate, returns new feature index
	size_t addFeature(const Coordinate coordinate);

	std::unique_ptr<MultiPointCollection> filter(const std::vector<bool> &keep);
	std::unique_ptr<MultiPointCollection> filter(const std::vector<char> &keep);

	std::string hash();

	virtual std::string toGeoJSON(bool displayMetadata);
	virtual std::string toCSV();

	virtual bool isSimple();

	virtual size_t getFeatureCount() const{
		return start_feature.size() - 1;
	}

	std::string getAsString();

	virtual ~MultiPointCollection(){};
};

#endif
