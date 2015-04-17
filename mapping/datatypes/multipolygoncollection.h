#ifndef DATATYPES_MULTIPOLYGONCOLLECTION_H_
#define DATATYPES_MULTIPOLYGONCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"

/**
 * This collection stores Multi-Polygons. Each Polygon consists of one outer and zero
 * or more inner rings (holes) that are stored in this order
 */
class MultiPolygonCollection : public SimpleFeatureCollection {
public:

	using SimpleFeatureCollection::SimpleFeatureCollection; //inherit constructor

	//starting index of individual rings in the points vector
	std::vector<uint32_t> start_ring;

	//starting index of individual polygons in the startRing vector
	std::vector<uint32_t> start_polygon;

	//starting index of individual Features in the startPolygon vector
	std::vector<uint32_t> start_feature;

	//return the index of the next feature in the startPolygon array that is no longer part of the index-th feature
	inline size_t stopFeature(size_t index) const {
		return index + 1 >= start_feature.size() ? start_polygon.size() : start_feature[index + 1];
	}

	inline size_t stopPolygon(size_t index) const {
		return index + 1 >= start_polygon.size() ? start_ring.size() : start_polygon[index + 1];
	}

	inline size_t stopRing(size_t index) const {
		size_t result =index + 1 >= start_ring.size() ? coordinates.size() : start_ring[index + 1];
		return result;
	}


	virtual std::string toGeoJSON(bool displayMetadata);
	virtual std::string toCSV();

	virtual bool isSimple();

	virtual ~MultiPolygonCollection(){};
};

#endif
