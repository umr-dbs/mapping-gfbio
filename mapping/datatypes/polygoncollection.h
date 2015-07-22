#ifndef DATATYPES_POLYGONCOLLECTION_H_
#define DATATYPES_POLYGONCOLLECTION_H_

#include "datatypes/simplefeaturecollection.h"
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
		start_feature.push_back(0); //end of first feature
		start_polygon.push_back(0); //end of first polygon
		start_ring.push_back(0); //end of first ring
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

	std::unique_ptr<PolygonCollection> filter(const std::vector<bool> &keep);
	std::unique_ptr<PolygonCollection> filter(const std::vector<char> &keep);

	virtual std::string toGeoJSON(bool displayMetadata) const;
	virtual std::string toCSV() const;
	virtual std::string toARFF() const;

	virtual std::string featureToWKT(size_t featureIndex) const;

	virtual bool isSimple() const final;

	virtual size_t getFeatureCount() const final {
		return start_feature.size() - 1;
	}

	virtual ~PolygonCollection(){};

	std::string getAsString();
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
		private:
			C &pc;
			const size_t idx;
	};

public:
	inline PolygonFeatureReference<PolygonCollection> getFeatureReference(size_t featureIndex){
		return PolygonFeatureReference<PolygonCollection>(*this, featureIndex);
	}

	inline PolygonFeatureReference<const PolygonCollection> getFeatureReference(size_t featureIndex) const{
		return PolygonFeatureReference<const PolygonCollection>(*this, featureIndex);
	}
};

#endif
