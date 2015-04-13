#ifndef DATATYPES_COLLECTION_H_
#define DATATYPES_COLLECTION_H_

#include <vector>
#include <string>
#include "datatypes/spatiotemporal.h"
#include "datatypes/attributes.h"

class Point {
	private:
		Point(double x, double y);
		Point(BinaryStream &stream);
		void toStream(BinaryStream &stream);
	public:
		Point() = delete;
		~Point();

		// Copy
		Point(const Point &p) = default;
		Point &operator=(const Point &p) = default;
		// Move
		Point(Point &&p) = default;
		Point &operator=(Point &&p) = default;

		double x, y;

		friend class MultiPointCollection;
		friend class GeosGeomUtil;
};


/**
 * Base class for collection data types (Point, Polygon, Line)
 */
//TODO: make class virtual
class SimpleFeatureCollection : public SpatioTemporalResult {
public:
	SimpleFeatureCollection(const SpatioTemporalReference &stref);
	virtual ~SimpleFeatureCollection();

	std::vector<Point> points;

	// Attributes
	std::vector<time_t> timestamps;

	// global MetaData (one value per PointCollection)
	const std::string &getGlobalMDString(const std::string &key) const;
	double getGlobalMDValue(const std::string &key) const;
	DirectMetadata<double>* getGlobalMDValueIterator();
	DirectMetadata<std::string>* getGlobalMDStringIterator();
	std::vector<std::string> getGlobalMDValueKeys() const;
	std::vector<std::string> getGlobalMDStringKeys() const;
	void setGlobalMDString(const std::string &key, const std::string &value);
	void setGlobalMDValue(const std::string &key, double value);

	// global MetaData (one value per collection)
	DirectMetadata<std::string> global_md_string;
	DirectMetadata<double> global_md_value;

	// local MetaData (one value per collection item)
	MetadataArrays<std::string> local_md_string;
	MetadataArrays<double> local_md_value;

	bool has_time;

	// Export
	virtual std::string toGeoJSON(bool displayMetadata = false) = 0;
	virtual std::string toCSV() = 0;

};

#endif
