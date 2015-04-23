#ifndef DATATYPES_POLYGONCOLLECTION_H_
#define DATATYPES_POLYGONCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"

/**
 * This collection stores Multi-Polygons. Each Polygon consists of one outer and zero
 * or more inner rings (holes) that are stored in this order
 */
class PolygonCollection : public SimpleFeatureCollection {
public:

	PolygonCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref){
		start_feature.push_back(0); //end of first feature
		start_polygon.push_back(0); //end of first polygon
		start_ring.push_back(0); //end of first ring
	}

	//starting index of individual rings in the points vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over rings has to stop at start_ring.size() -2
	std::vector<uint32_t> start_ring;

	//starting index of individual polygons in the startRing vector, last entry indicates first index out of bounds of start_ring
	//thus iterating over polygons has to stop at start_polygon.size() -2
	std::vector<uint32_t> start_polygon;

	//starting index of individual Features in the startPolygon vector, last entry indicates first index out of bounds of start_polygon
	//thus iterating over features has to stop at start_feature.size() -2
	std::vector<uint32_t> start_feature;

	//add a new coordinate, to a new ring. After adding all coordinates, finishRing() has to be called
	void addCoordinate(double x, double y);
	//finishes the definition of the new ring, returns new ring index
	size_t finishRing();
	//finishes the definition of the new polygon, returns new polygon index
	size_t finishPolygon();
	//finishes the definition of the new feature, returns new feature index
	size_t finishFeature();

	virtual std::string toGeoJSON(bool displayMetadata) const;
	virtual std::string toCSV() const;

	virtual bool isSimple() const final;

	virtual size_t getFeatureCount() const final{
		return start_feature.size() - 1;
	}

	virtual ~PolygonCollection(){};

	std::string getAsString();
};

#endif
