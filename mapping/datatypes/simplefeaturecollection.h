#ifndef DATATYPES_COLLECTION_H_
#define DATATYPES_COLLECTION_H_

#include <vector>
#include <string>
#include "datatypes/spatiotemporal.h"
#include "datatypes/attributes.h"

class Coordinate {
	private:
		Coordinate(BinaryStream &stream);
		void toStream(BinaryStream &stream);
	public:
		Coordinate(double x, double y) : x(x), y(y) {}

		Coordinate() = delete;
		~Coordinate() {}

		// Copy
		Coordinate(const Coordinate &p) = default;
		Coordinate &operator=(const Coordinate &p) = default;
		// Move
		Coordinate(Coordinate &&p) = default;
		Coordinate &operator=(Coordinate &&p) = default;

		double x, y;

		friend class PointCollection;
		friend class GeosGeomUtil;
};


/**
 * Base class for collection data types (Point, Polygon, Line)
 */
class SimpleFeatureCollection : public SpatioTemporalResult {
public:
	SimpleFeatureCollection(const SpatioTemporalReference &stref) : SpatioTemporalResult(stref) {}

	virtual ~SimpleFeatureCollection() {}

	std::vector<Coordinate> coordinates;

	// Timestamps
	std::vector<time_t> time_start;
	std::vector<time_t> time_end;
	bool hasTime() const;
	void addDefaultTimestamps();
	void addDefaultTimestamps(double min, double max);

	// global MetaData (one value per SimpleFeatureCollection)
	const std::string &getGlobalMDString(const std::string &key) const;
	double getGlobalMDValue(const std::string &key) const;
	DirectMetadata<double>* getGlobalMDValueIterator();
	DirectMetadata<std::string>* getGlobalMDStringIterator();
	std::vector<std::string> getGlobalMDValueKeys() const;
	std::vector<std::string> getGlobalMDStringKeys() const;
	void setGlobalMDString(const std::string &key, const std::string &value);
	void setGlobalMDValue(const std::string &key, double value);

	// global MetaData (one value per feature)
	DirectMetadata<std::string> global_md_string;
	DirectMetadata<double> global_md_value;

	// local MetaData (one value per feature)
	MetadataArrays<std::string> local_md_string;
	MetadataArrays<double> local_md_value;

	// Export
	virtual std::string toGeoJSON(bool displayMetadata = false) const = 0;
	virtual std::string toCSV() const = 0;
	virtual std::string toWKT() const;
	virtual std::string toARFF(std::string layerName = "export") const;

	virtual std::string featureToWKT(size_t featureIndex) const;

	// return true if all features consist of a single element
	virtual bool isSimple() const = 0;

	virtual size_t getFeatureCount() const = 0;

protected:
	virtual void featureToWKT(size_t featureIndex, std::ostringstream& wkt) const = 0;

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
