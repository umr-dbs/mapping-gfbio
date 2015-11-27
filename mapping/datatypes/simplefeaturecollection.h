#ifndef DATATYPES_COLLECTION_H_
#define DATATYPES_COLLECTION_H_

#include "datatypes/spatiotemporal.h"
#include "datatypes/attributes.h"

#include <vector>
#include <string>
#include <limits>


class Coordinate {
	private:
		Coordinate(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;
	public:
		Coordinate(double x, double y) : x(x), y(y) {}

		Coordinate() = delete;
		~Coordinate() = default;

		// Copy
		Coordinate(const Coordinate &p) = default;
		Coordinate &operator=(const Coordinate &p) = default;
		// Move
		Coordinate(Coordinate &&p) = default;
		Coordinate &operator=(Coordinate &&p) = default;

		bool almostEquals(const Coordinate& coordinate) const {
			return std::abs(x - coordinate.x) < std::numeric_limits<double>::epsilon() && std::abs(y - coordinate.y) < std::numeric_limits<double>::epsilon();
		}

		double x, y;

		friend class PointCollection;
		friend class LineCollection;
		friend class PolygonCollection;
		friend class GeosGeomUtil;
};


/**
 * Base class for collection data types (Point, Polygon, Line)
 */
class SimpleFeatureCollection : public SpatioTemporalResult {
public:
	SimpleFeatureCollection(const SpatioTemporalReference &stref) : SpatioTemporalResult(stref) {}

	// allow move construction and move assignment
	SimpleFeatureCollection(SimpleFeatureCollection &&) = default;
	SimpleFeatureCollection& operator=(SimpleFeatureCollection &&) = default;

	virtual ~SimpleFeatureCollection() {}

	std::vector<Coordinate> coordinates;

	// Timestamps
	std::vector<double> time_start;
	std::vector<double> time_end;
	bool hasTime() const;
	void addDefaultTimestamps();
	void addDefaultTimestamps(double min, double max);

	// feature attributes (one value per feature)
	AttributeArrays feature_attributes;

	// geometry
	virtual SpatialReference getCollectionMBR() const;
	virtual SpatialReference getFeatureMBR(size_t featureIndex) const = 0;

	/**
	 * check whether feature intersects with given rectangle
	 * @param featureIndex index of the feate
	 * @param x1 x of upper left coordinate of rectangle
	 * @param y1 y of upper left coordiante of rectangle
	 * @param x2 y of lower right coordinate of rectangle
	 * @param y2 x of lower right coordinate of rectangle
	 * @return true if feature with given index intersects given rectangle
	 */
	virtual bool featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const = 0;

	/**
	 * filter collection by a given spatial reference
	 * @param sref spatial reference
	 * @return new collection that contains only features that intersect with rectangle given by sref
	 */
	bool featureIntersectsRectangle(size_t featureIndex, const SpatialReference& sref) const;

	// Export
	std::string toGeoJSON(bool displayMetadata = false) const;
	virtual std::string toCSV() const = 0;
	std::string toWKT() const;
	virtual std::string toARFF(std::string layerName = "export") const;

	virtual std::string featureToWKT(size_t featureIndex) const;

	// return true if all features consist of a single element
	virtual bool isSimple() const = 0;

	virtual size_t getFeatureCount() const = 0;

	void validate() const;

protected:
	virtual void featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const = 0;
	virtual void featureToWKT(size_t featureIndex, std::ostringstream& wkt) const = 0;

	virtual void validateSpecifics() const = 0;

	//calculate the MBR of the coordinates in range from start to stop (exclusive)
	SpatialReference calculateMBR(size_t coordinateIndexStart, size_t coordinateIndexStop) const;

	// helper for filterBySpatioTemporalReferenceIntersection() implemented in the child classes
	std::vector<bool> getKeepVectorForFilterBySpatioTemporalReferenceIntersection(const SpatioTemporalReference& stref) const;

	//geometry helper functions

	/**
	 * check if two line segments intersect
	 * @param p1 first point of first line
	 * @param p2 second point of first line
	 * @param p3 first point of first line
	 * @param p4 second point of first line
	 * @return true if the two line segments intersect
	 */
	bool lineSegmentsIntersect(const Coordinate& p1, const Coordinate& p2, const Coordinate& p3, const Coordinate& p4) const;


	/*
	 * Helper classes for iteration over Collections
	 */
	template <typename Collection, template<typename> class value_type>
	class SimpleFeatureIterator {
		public:
			SimpleFeatureIterator(Collection &sfc, size_t idx) : sfc(sfc), idx(idx) {
			};

	        bool operator!=(const SimpleFeatureIterator &other) const {
	            return idx != other.idx;
	        }

	        value_type<Collection> operator*() const {
	            return value_type<Collection>(sfc, idx);
	        }

	        void operator++() {
	            idx++;
	        }

		private:
			Collection &sfc;
			size_t idx;
	};
};



#endif
