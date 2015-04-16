#ifndef DATATYPES_COLLECTION_H_
#define DATATYPES_COLLECTION_H_

#include <vector>
#include <string>
#include "datatypes/spatiotemporal.h"
#include "datatypes/attributes.h"

class Coordinate {
	private:
		Coordinate(double x, double y);
		Coordinate(BinaryStream &stream);
		void toStream(BinaryStream &stream);
	public:
		Coordinate() = delete;
		~Coordinate();

		// Copy
		Coordinate(const Coordinate &p) = default;
		Coordinate &operator=(const Coordinate &p) = default;
		// Move
		Coordinate(Coordinate &&p) = default;
		Coordinate &operator=(Coordinate &&p) = default;

		double x, y;

		friend class MultiPointCollection;
		friend class GeosGeomUtil;
};


/**
 * Base class for collection data types (Point, Polygon, Line)
 */
class SimpleFeatureCollection : public SpatioTemporalResult {
public:
	SimpleFeatureCollection(const SpatioTemporalReference &stref);
	virtual ~SimpleFeatureCollection();

	std::vector<Coordinate> coordinates;

	// Attributes
	std::vector<time_t> timestamps;

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

	bool has_time;

	// Export
	virtual std::string toGeoJSON(bool displayMetadata = false) = 0;
	virtual std::string toCSV() = 0;

};

#endif
