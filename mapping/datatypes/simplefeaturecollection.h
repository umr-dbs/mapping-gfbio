#ifndef DATATYPES_COLLECTION_H_
#define DATATYPES_COLLECTION_H_

#include "datatypes/spatiotemporal.h"
#include "datatypes/attributes.h"

#include <vector>
#include <string>
#include <limits>


class Coordinate {
	public:
		Coordinate(BinaryReadBuffer &buffer);
		void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

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
	std::vector<TimeInterval> time;

	/**
	 * check if the features in the collection have associated timestamps
	 * @return true if all features in the collection have associated timestamps
	 */
	bool hasTime() const;

	/**
	 * Set timestamps for features in this collection. Length of vectors must be equal to getFeatureCount
	 * @param time_start the start values for the features
	 * @param time_end the end values for the features
	 */
	void setTimeStamps(std::vector<double> &&time_start, std::vector<double> &&time_end);

	/**
	 * add default timestamps [minDouble, maxDouble) for all features in the collection
	 */
	void addDefaultTimestamps();

	/**
	 * add given timestamps for all features in the collection
	 * @param min the time_start for all features
	 * @param max the time_end for all features
	 */
	void addDefaultTimestamps(double min, double max);

	void addGlobalAttributesFromCollection(const SimpleFeatureCollection &collection);

	void addFeatureAttributesFromCollection(const SimpleFeatureCollection &collection);

	// feature attributes (one value per feature)
	AttributeArrays feature_attributes;

	// geometry
	/**
	 * get the minimum bounding rectangle of this collection as a SpatialReference
	 * @return the minimum bounding rectangle of this collection as a SpatialReference
	 */
	virtual SpatialReference getCollectionMBR() const;

	/**
	 * get the minimum bounding rectangle of given feature in this collection
	 * @param featureIndex the index of the feature
	 * @return the minimum bounding rectangle of a feature in this collection as a SpatialReference
	 */
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
	/**
	 * Get a representation of the collection as GeoJSON
	 * @param displayMetadata if true, include attributes
	 * @return a GeoJSON representation of this collection
	 */
	std::string toGeoJSON(bool displayMetadata = false) const;

	/**
	 * Get a CSV representation of this collection
	 * @return a CSV representation of this collection
	 */
	virtual std::string toCSV() const;

	/**
	 * Get a WKT representation of this collection
	 * @eturn a WKT representation of this collection
	 */
	std::string toWKT() const;

	/**
	 * Get a ARFF representation of this collection
	 * @param layerName the name of the relation in the ARFF file
	 * @return a ARFF representation of this collection
	 */
	virtual std::string toARFF(std::string layerName = "export") const;

	/**
	 * Get a WKT representation of a given feature in this collection
	 * @param featureIndex the index of the feature
	 * @return a WKT representation of a given feature in this collection
	 */
	virtual std::string featureToWKT(size_t featureIndex) const;

	/**
	 * Determine if collection is simple
	 * @return true if all features consist of a single element
	 */
	virtual bool isSimple() const = 0;

	/**
	 * Get the number of features in this collection
	 * @return the number of features in this collection
	 */
	virtual size_t getFeatureCount() const = 0;

	/**
	 * Remove the last feature in this collection.
	 * If the collection has an unfinished feature (finishFeature() not called) this will be removed instead.
	 * If the collection is empty, nothing happens.
	 */
	virtual void removeLastFeature() = 0;

	/**
	 * Validate the contents of this collection. Ensure that the last feature is finished, feature_attributes and time information are in check.
	 * This function must be called after finishing the construction of a collection
	 */
	void validate() const;

	/**
	 * the size of this object in memory (in bytes)
	 * @return the size of this object in bytes
	 */
	virtual size_t get_byte_size() const;

protected:
	virtual void featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const = 0;
	virtual void featureToWKT(size_t featureIndex, std::ostringstream& wkt) const = 0;

	virtual void validateSpecifics() const = 0;

	//calculate the MBR of the coordinates in range from start to stop (exclusive)
	SpatialReference calculateMBR(size_t coordinateIndexStart, size_t coordinateIndexStop) const;

	size_t calculate_kept_count(const std::vector<bool> &keep) const;
	size_t calculate_kept_count(const std::vector<char> &keep) const;

	// helper for filterBySpatioTemporalReferenceIntersection() implemented in the child classes
	std::vector<bool> getKeepVectorForFilterBySpatioTemporalReferenceIntersection(const SpatioTemporalReference& stref) const;

	//helper for addFeatureFromCollection
	void setAttributesAndTimeFromCollection(const SimpleFeatureCollection &collection, size_t collectionIndex, size_t thisIndex, const std::vector<std::string> &textualAttributes, const std::vector<std::string> &numericAttributes);


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
