#ifndef DATATYPES_POLYGONCOLLECTION_H_
#define DATATYPES_POLYGONCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"
#include "util/exceptions.h"
#include <memory>


/**
 * This collection contains Polygon-Features. Each Feature consists of one or more polygons.
 * Each Polygon consists of one outer and zero or more inner rings (holes) that are stored in this order.
 */
class PolygonCollection : public SimpleFeatureCollection {
private:
	template<typename C> class PolygonFeatureReference;
	template<typename C> class PolygonPolygonReference;
	template<typename C> class PolygonRingReference;
public:
	PolygonCollection(const SpatioTemporalReference &stref) : SimpleFeatureCollection(stref){
		start_feature.push_back(0); //start of first feature
		start_polygon.push_back(0); //start of first polygon
		start_ring.push_back(0); //start of first ring
	}

	typedef SimpleFeatureIterator<PolygonCollection, PolygonFeatureReference> iterator;
	typedef SimpleFeatureIterator<const PolygonCollection, PolygonFeatureReference> const_iterator;

    inline iterator begin() {
    	return iterator(*this, 0);
    }
    inline iterator end() {
    	return iterator(*this, getFeatureCount());
    }
    inline const_iterator begin() const {
    	return const_iterator(*this, 0);
    }
    inline const_iterator end() const {
    	return const_iterator(*this, getFeatureCount());
    }

	//starting index of individual rings in the coordinates vector, last entry indicates first index out of bounds of coordinates
	//thus iterating over rings has to stop at start_ring.size() -2
	std::vector<uint32_t> start_ring;

	//starting index of individual polygons in the start_ring vector, last entry points to start_ring entry that marks the end of the very last ring
	//thus iterating over polygons has to stop at start_polygon.size() -2
	std::vector<uint32_t> start_polygon;

	//starting index of individual Features in the start_polygon vector, last entry points to start_polygon entry that marks the end of the very last polygon
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

	std::unique_ptr<PolygonCollection> filter(const std::vector<bool> &keep);
	std::unique_ptr<PolygonCollection> filter(const std::vector<char> &keep);

	/**
	 * filter collection by a given rectangle
	 * @param x1 x of upper left coordinate of rectangle
	 * @param y1 y of upper left coordiante of rectangle
	 * @param x2 y of lower right coordinate of rectangle
	 * @param y2 x of lower right coordinate of rectangle
	 * @return new collection that contains only features that intersect with rectangle
	 */
	virtual std::unique_ptr<PolygonCollection> filterByRectangleIntersection(double x1, double y1, double x2, double y2);

	/**
	 * filter collection by a given spatial reference
	 * @param sref spatial reference
	 * @return new collection that contains only features that intersect with rectangle given by sref
	 */
	virtual std::unique_ptr<PolygonCollection> filterByRectangleIntersection(const SpatialReference& sref);

	virtual bool featureIntersectsRectangle(size_t featureIndex, double x1, double y1, double x2, double y2) const;

	virtual SpatialReference getCollectionMBR() const;
	virtual SpatialReference getFeatureMBR(size_t featureIndex) const;

	//if point is exactly on edge it may return true or false
	//coordinateIndexStop is exclusive
	bool pointInRing(const Coordinate& coordinate, size_t coordinateIndexStart, size_t coordinateIndexStop) const;

	bool pointInCollection(Coordinate& coordinate) const;

	std::string hash();

	virtual std::string toCSV() const;

	virtual bool isSimple() const final;

	virtual size_t getFeatureCount() const final {
		return start_feature.size() - 1;
	}

	virtual ~PolygonCollection(){};

	std::string getAsString();

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
	class PolygonFeatureReference {
		public:
			PolygonFeatureReference(C &pc, size_t idx) : pc(pc), idx(idx) {};

			typedef SimpleFeatureIterator<C, PolygonPolygonReference> iterator;
			typedef SimpleFeatureIterator<const C, PolygonPolygonReference> const_iterator;

		    iterator begin() {
		    	return iterator(pc, pc.start_feature[idx]);
		    }
		    iterator end() {
		    	return iterator(pc, pc.start_feature[idx+1]);
		    }

		    const_iterator begin() const {
		    	return const_iterator(pc, pc.start_feature[idx]);
		    }
		    const_iterator end() const {
		    	return const_iterator(pc, pc.start_feature[idx+1]);
		    }

		    size_t size() const {
		    	return pc.start_feature[idx+1] - pc.start_feature[idx];
		    }

		    operator size_t() const {
		    	return idx;
		    }

		    inline PolygonPolygonReference<PolygonCollection> getPolygonReference(size_t polygonIndex){
		    	if(polygonIndex >= size())
		    		throw ArgumentException("polygonIndex >= Count");
				return PolygonPolygonReference<PolygonCollection>(pc, pc.start_feature[idx] + polygonIndex);
			}

			inline PolygonPolygonReference<const PolygonCollection> getPolygonReference(size_t polygonIndex) const{
				if(polygonIndex >= size())
					throw ArgumentException("polygonIndex >= Count");
				return PolygonPolygonReference<const PolygonCollection>(pc, pc.start_feature[idx] + polygonIndex);
			}

			bool contains(Coordinate& coordinate) const {
				for(auto polygon : *this){
					if(polygon.contains(coordinate))
						return true;
				}
				return false;
			}

			SpatialReference getMBR() const {
				//TODO: compute MBRs of outer rings of all polygons and then the MBR of these MBRs?
				return pc.calculateMBR(pc.start_ring[pc.start_polygon[pc.start_feature[idx]]], pc.start_ring[pc.start_polygon[pc.start_feature[idx + 1]]]);
			}

		private:
		    C &pc;
			const size_t idx;
	};
	template<typename C>
	class PolygonPolygonReference {
		public:
			PolygonPolygonReference(C &pc, size_t idx) : pc(pc), idx(idx) {};

			typedef SimpleFeatureIterator<C, PolygonRingReference> iterator;
			typedef SimpleFeatureIterator<const C, PolygonRingReference> const_iterator;

		    iterator begin() {
		    	return iterator(pc, pc.start_polygon[idx]);
		    }
		    iterator end() {
		    	return iterator(pc, pc.start_polygon[idx+1]);
		    }

		    const_iterator begin() const {
		    	return const_iterator(pc, pc.start_polygon[idx]);
		    }
		    const_iterator end() const {
		    	return const_iterator(pc, pc.start_polygon[idx+1]);
		    }

		    size_t size() const {
		    	return pc.start_polygon[idx+1] - pc.start_polygon[idx];
		    }

		    /**
		     * return the index of the current polygon in the start_polygon array
		     */
		    size_t getPolygonIndex() const {
				return idx;
			}

			inline PolygonRingReference<PolygonCollection> getRingReference(size_t ringIndex){
				if(ringIndex >= size())
					throw ArgumentException("RingIndex >= Count");
				return PolygonRingReference<PolygonCollection>(pc, pc.start_polygon[idx] + ringIndex);
			}

			inline PolygonRingReference<const PolygonCollection> getRingReference(size_t ringIndex) const{
				if(ringIndex >= size())
					throw ArgumentException("RingIndex >= Count");
				return PolygonRingReference<const PolygonCollection>(pc, pc.start_polygon[idx] + ringIndex);
			}

			bool contains(Coordinate& coordinate) const {
				bool outerRing = true;
				for(auto ring : *this){
					if(outerRing){
						if(!ring.contains(coordinate))
							return false;
					}
					else if(ring.contains(coordinate))
						return false;

					outerRing = false;
				}

				return true;
			}

			SpatialReference getMBR() const {
				return pc.calculateMBR(pc.start_ring[pc.start_polygon[idx]], pc.start_ring[pc.start_polygon[idx] + 1]);
			}

		private:
		    C &pc;
			const size_t idx;
	};
	template<typename C>
	class PolygonRingReference {
		public:
			PolygonRingReference(C &pc, size_t idx) : pc(pc), idx(idx) {};

			typedef decltype(std::declval<C>().coordinates.begin()) iterator;
			typedef decltype(std::declval<C>().coordinates.cbegin()) const_iterator;

		    iterator begin() {
		    	return std::next(pc.coordinates.begin(), pc.start_ring[idx]);
		    }
		    iterator end() {
		    	return std::next(pc.coordinates.begin(), pc.start_ring[idx+1]);
		    }

		    const_iterator begin() const {
		    	return std::next(pc.coordinates.begin(), pc.start_ring[idx]);
		    }
		    const_iterator end() const {
		    	return std::next(pc.coordinates.begin(), pc.start_ring[idx+1]);
		    }

		    size_t size() const {
		    	return pc.start_ring[idx+1] - pc.start_ring[idx];
		    }

		    /**
			 * return the index of the current ring in the start_ring array
			 */
		    size_t getRingIndex() const {
				return idx;
			}

		    bool contains(Coordinate& coordinate) const {
		    	return pc.pointInRing(coordinate, pc.start_ring[idx], pc.start_ring[idx+1]);
		    }

			SpatialReference getMBR() const {
				return pc.calculateMBR(pc.start_ring[idx], pc.start_ring[idx + 1]);
			}

		private:
			C &pc;
			const size_t idx;
	};

	/**
	 * This class should be used to test many points for containment in a PolygonCollection
	 * on instantiation it performs pre-calculations in order to make tests faster
	 * if the corresponding PolygonCollection is changed the results will be faulty
	 */
	class PointInCollectionBulkTester {
	public:
		PointInCollectionBulkTester(const PolygonCollection& polygonCollection);

		bool pointInCollection(const Coordinate& coordinate) const;

	private:
		const PolygonCollection& polygonCollection;
		std::vector<double> constants, multiples;

		void performPrecalculation();
		void precalculateRing(size_t coordinateIndexStart, size_t coordinateIndexStop);
		bool pointInRing(const Coordinate& coordinate, size_t coordinateIndexStart, size_t coordinateIndexStop) const;
	};

public:
	inline PolygonFeatureReference<PolygonCollection> getFeatureReference(size_t featureIndex){
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return PolygonFeatureReference<PolygonCollection>(*this, featureIndex);
	}

	inline PolygonFeatureReference<const PolygonCollection> getFeatureReference(size_t featureIndex) const{
		if(featureIndex >= getFeatureCount())
			throw ArgumentException("FeatureIndex >= FeatureCount");
		return PolygonFeatureReference<const PolygonCollection>(*this, featureIndex);
	}

	PointInCollectionBulkTester getPointInCollectionBulkTester() const {
		return PointInCollectionBulkTester(*this);
	}
};

#endif
