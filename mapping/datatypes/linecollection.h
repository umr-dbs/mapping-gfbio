#ifndef DATATYPES_LINECOLLECTION_H_
#define DATATYPES_LINECOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"
#include "util/exceptions.h"
#include <memory>


/**
 * This collection contains Multi-Lines
 */
class LineCollection : public SimpleFeatureCollection {
private:
	template<typename C> class LineFeatureReference;
	template<typename C> class LineLineReference;
public:
	LineCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref) {
		start_feature.push_back(0); //start of first feature
		start_line.push_back(0); //start of first line
	}

	LineCollection(BinaryStream& stream);

	typedef SimpleFeatureIterator<LineCollection, LineFeatureReference> iterator;
	typedef SimpleFeatureIterator<const LineCollection, LineFeatureReference> const_iterator;

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

	//starting index of individual lines in the coordinates vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over lines has to stop at start_line.size() -2
	std::vector<uint32_t> start_line;

	//starting index of individual features in the start_line vector, last entry points to start_line entry that marks the end of the last line
	//thus iterating over features has to stop at start_feature.size() -2
	std::vector<uint32_t> start_feature;

	/**
	 * Serialize collection to stream
	 * @param stream the stream to serialize to
	 */
	void toStream(BinaryStream &stream) const;

	/**
	 * add a new coordinate to the current line. After adding all coordinates, finishLine() has to be called
	 * @param x the x value of the coordinate
	 * @param y the y value of the coordinate
	 */
	void addCoordinate(double x, double y);

	/**
	 * finishes the definition of the new line
	 * @return index of the new line
	 */
	size_t finishLine();

	/**
	 * finishes the definition of the new feature
	 * @return index of the new feature
	 */
	size_t finishFeature();

	/**
	 * filter the features of the collections based on keep vector
	 * @param keep the vector specifying which features to keep
	 * @return new collection containing only the features that should be kept
	 */
	std::unique_ptr<LineCollection> filter(const std::vector<bool> &keep) const;

	/**
	 * filter the features of the collections based on keep vector
	 * @param keep the vector specifying which features to keep
	 * @return new collection containing only the features that should be kept
	 */
	std::unique_ptr<LineCollection> filter(const std::vector<char> &keep) const;

	/**
	 * filter collection by a given rectangle
	 * @param x1 x of upper left coordinate of rectangle
	 * @param y1 y of upper left coordiante of rectangle
	 * @param x2 y of lower right coordinate of rectangle
	 * @param y2 x of lower right coordinate of rectangle
	 * @return new collection that contains only features that intersect with rectangle
	 */
	virtual std::unique_ptr<LineCollection> filterByRectangleIntersection(double x1, double y1, double x2, double y2) const;

	/**
	 * filter collection by a given spatial reference
	 * @param sref spatial reference
	 * @return new collection that contains only features that intersect with rectangle given by sref
	 */
	virtual std::unique_ptr<LineCollection> filterByRectangleIntersection(const SpatialReference& sref) const;

	virtual bool featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const;

	virtual SpatialReference getFeatureMBR(size_t featureIndex) const;

	virtual std::string toCSV() const;

	virtual bool isSimple() const;

	virtual size_t getFeatureCount() const {
		return start_feature.size() - 1;
	}

	virtual ~LineCollection(){};

	LineCollection& operator+=( const LineCollection &other );

protected:
	virtual void featureToGeoJSONGeometry(size_t featureIndex, std::ostringstream& json) const;
	virtual void featureToWKT(size_t featureIndex, std::ostringstream& wkt) const;

	virtual void validateSpecifics() const;

private:

	/*
	 * Finally, implement the helper classes for iteration.
	 * Yes, this is a bit verbose. Can't be helped.
	 */
	template<typename C>
	class LineFeatureReference {
		public:
			LineFeatureReference(C &lc, size_t idx) : lc(lc), idx(idx) {};

			typedef SimpleFeatureIterator<C, LineLineReference> iterator;
			typedef SimpleFeatureIterator<const C, LineLineReference> const_iterator;

		    iterator begin() {
		    	return iterator(lc, lc.start_feature[idx]);
		    }
		    iterator end() {
		    	return iterator(lc, lc.start_feature[idx+1]);
		    }

		    const_iterator begin() const {
		    	return const_iterator(lc, lc.start_feature[idx]);
		    }
		    const_iterator end() const {
		    	return const_iterator(lc, lc.start_feature[idx+1]);
		    }

		    size_t size() const {
		    	return lc.start_feature[idx+1] - lc.start_feature[idx];
		    }

		    operator size_t() const {
		    	return idx;
		    }

		    SpatialReference getMBR() const{
		    	return lc.calculateMBR(lc.start_line[lc.start_feature[idx]], lc.start_line[lc.start_feature[idx + 1]]);
		    }

		    inline LineLineReference<C> getLineReference(size_t lineIndex){
		    	if(lineIndex >= size())
		    		throw ArgumentException("LineIndex >= Count");
		    	return LineLineReference<C>(lc, lc.start_feature[idx] + lineIndex);
			}

			inline LineLineReference<const C> getLineReference(size_t lineIndex) const{
				if(lineIndex >= size())
					throw ArgumentException("LineIndex >= Count");
				return LineLineReference<const C>(lc, lc.start_feature[idx] + lineIndex);
			}

		private:
		    C &lc;
			const size_t idx;
	};
	template<typename C>
	class LineLineReference {
		public:
			LineLineReference(C &lc, size_t idx) : lc(lc), idx(idx) {};

			typedef decltype(std::declval<C>().coordinates.begin()) iterator;
			typedef decltype(std::declval<C>().coordinates.cbegin()) const_iterator;

		    iterator begin() {
		    	return std::next(lc.coordinates.begin(), lc.start_line[idx]);
		    }
		    iterator end() {
		    	return std::next(lc.coordinates.begin(), lc.start_line[idx+1]);
		    }

		    const_iterator begin() const {
		    	return std::next(lc.coordinates.begin(), lc.start_line[idx]);
		    }
		    const_iterator end() const {
		    	return std::next(lc.coordinates.begin(), lc.start_line[idx+1]);
		    }

		    size_t size() const {
		    	return lc.start_line[idx+1] - lc.start_line[idx];
		    }

		    /**
			 * return the index of the current line in the start_line array
			 */
			size_t getLineIndex() const {
				return idx;
			}

		    SpatialReference getMBR() const {
		    	return lc.calculateMBR(lc.start_line[idx], lc.start_line[idx + 1]);
		    }

		private:
			C &lc;
			const size_t idx;
	};

public:
	inline LineFeatureReference<LineCollection> getFeatureReference(size_t featureIndex){
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return LineFeatureReference<LineCollection>(*this, featureIndex);
	}

	inline LineFeatureReference<const LineCollection> getFeatureReference(size_t featureIndex) const{
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return LineFeatureReference<const LineCollection>(*this, featureIndex);
	}
};

#endif
