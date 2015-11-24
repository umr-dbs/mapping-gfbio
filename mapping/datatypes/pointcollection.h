#ifndef DATATYPES_POINTCOLLECTION_H_
#define DATATYPES_POINTCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"
#include "util/exceptions.h"
#include <memory>


/**
 * This collection contains Point-Features, each feature consisting of one or more points
 */
class PointCollection : public SimpleFeatureCollection {
private:
	template<typename C> class PointFeatureReference;
public:

	/**
	 * Create PointCollection by deserializing from a stream
	 * @param stream the stream to deserialize collection from
	 */
	PointCollection(BinaryStream &stream);

	/**
	 * Create PointCollection with given SpatioTemporalReference
	 * @param stref the SpatioTemporalReference for this collection
	 */
	PointCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref) {
		start_feature.push_back(0); //start of first feature
	}

	typedef SimpleFeatureIterator<PointCollection, PointFeatureReference> iterator;
	typedef SimpleFeatureIterator<const PointCollection, PointFeatureReference> const_iterator;

	/**
	 * Get iterator to beginning of features of the collection
	 * @return iterator to beginning of features of the collection
	 */
    inline iterator begin() {
    	return iterator(*this, 0);
    }

	/**
	 * Get iterator to end of features of the collection
	 * @return iterator to end of features of the collection
	 */
    inline iterator end() {
    	return iterator(*this, getFeatureCount());
    }

	/**
	 * Get const iterator to beginning of features of the collection
	 * @return const iterator to beginning of features of the collection
	 */
    inline const_iterator begin() const {
    	return const_iterator(*this, 0);
    }

	/**
	 * Get const iterator to end of features of the collection
	 * @return const iterator to end of features of the collection
	 */
    inline const_iterator end() const {
    	return const_iterator(*this, getFeatureCount());
    }

	//starting index of individual features in the coordinates vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over features has to stop at start_feature.size() -2
	std::vector<uint32_t> start_feature;

	/**
	 * Serialize collection to stream
	 * @param stream the stream to serialize to
	 */
	void toStream(BinaryStream &stream) const;

	/**
	 * add a new coordinate to the current feature. After adding all coordinates, finishFeature() has to be called
	 * @param x the x value of the coordinate
	 * @param y the y value of the coordinate
	 */
	void addCoordinate(double x, double y);

	/**
	 * finishes the definition of the new feature
	 * @return index of the new feature
	 */
	size_t finishFeature();

	/**
	 * add a new feature consisting of a single coordinate
	 * @param coordinate the coordinate of the feature
	 * @return index of the new feature
	 */
	size_t addSinglePointFeature(const Coordinate coordinate);

	/**
	 * filter the features of the collections based on keep vector
	 * @param keep the vector specifying which features to keep
	 * @return new collection containing only the features that should be kept
	 */
	std::unique_ptr<PointCollection> filter(const std::vector<bool> &keep) const;

	/**
	 * filter the features of the collections based on keep vector
	 * @param keep the vector specifying which features to keep
	 * @return new collection containing only the features that should be kept
	 */
	std::unique_ptr<PointCollection> filter(const std::vector<char> &keep) const;

	/**
	 * filter collection by a given rectangle
	 * @param x1 x of upper left coordinate of rectangle
	 * @param y1 y of upper left coordiante of rectangle
	 * @param x2 y of lower right coordinate of rectangle
	 * @param y2 x of lower right coordinate of rectangle
	 * @return new collection that contains only features that intersect with rectangle
	 */
	virtual std::unique_ptr<PointCollection> filterByRectangleIntersection(double x1, double y1, double x2, double y2) const;

	/**
	 * filter collection by a given spatial reference
	 * @param sref spatial reference
	 * @return new collection that contains only features that intersect with rectangle given by sref
	 */
	virtual std::unique_ptr<PointCollection> filterByRectangleIntersection(const SpatialReference& sref) const;

	virtual bool featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const;

	/**
	 * compute the hash of the collection
	 * @return the hash of the collection
	 */
	std::string hash();

	virtual SpatialReference getFeatureMBR(size_t featureIndex) const;

	virtual std::string toCSV() const;
	virtual std::string toARFF(std::string layerName = "export") const;

	virtual bool isSimple() const final;

	virtual size_t getFeatureCount() const final {
		return start_feature.size() - 1;
	}

	std::string getAsString();

	virtual ~PointCollection(){};

protected:
	virtual void featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const;
	virtual void featureToWKT(size_t featureIndex, std::ostringstream& wkt) const;

	virtual void validateSpecifics() const;

private:
	/*
	 * Finally, implement the helper classes for iteration
	 */
	template<typename C>
	class PointFeatureReference {
		public:
			PointFeatureReference(C &pc, size_t idx) : pc(pc), idx(idx) {};

			typedef decltype(std::declval<C>().coordinates.begin()) iterator;
			typedef decltype(std::declval<C>().coordinates.cbegin()) const_iterator;

			iterator begin() {
		    	return std::next(pc.coordinates.begin(), pc.start_feature[idx]);
		    }
			iterator end() {
		    	return std::next(pc.coordinates.begin(), pc.start_feature[idx+1]);
		    }

		    const_iterator begin() const {
		    	return std::next(pc.coordinates.cbegin(), pc.start_feature[idx]);
		    }
		    const_iterator end() const {
		    	return std::next(pc.coordinates.cbegin(), pc.start_feature[idx+1]);
		    }

		    size_t size() const {
		    	return pc.start_feature[idx+1] - pc.start_feature[idx];
		    }

		    operator size_t() const {
		    	return idx;
		    }

		    SpatialReference getMBR() const {
		    	return pc.calculateMBR(pc.start_feature[idx], pc.start_feature[idx+1]);
		    }

		private:
			C &pc;
			const size_t idx;
	};
public:
	inline PointFeatureReference<PointCollection> getFeatureReference(size_t featureIndex){
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return PointFeatureReference<PointCollection>(*this, featureIndex);
	}

	inline PointFeatureReference<const PointCollection> getFeatureReference(size_t featureIndex) const{
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return PointFeatureReference<const PointCollection>(*this, featureIndex);
	}
};


#endif
