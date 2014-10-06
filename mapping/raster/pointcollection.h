#ifndef RASTER_POINTCOLLECTION_H
#define RASTER_POINTCOLLECTION_H

#include "raster/raster.h"
#include "raster/metadata.h"
#include <vector>
#include <map>
#include <string>
#include <sys/types.h>

class Socket;

class Point {
	private:
		Point(double x, double y);
		Point(Socket &socket);
		void toSocket(Socket &socket);
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

		friend class PointCollection;
};


class PointCollection {
	public:
		PointCollection(epsg_t epsg = EPSG_UNKNOWN);
		PointCollection(Socket &socket);
		~PointCollection();

		std::unique_ptr<PointCollection> filter(const std::vector<bool> &keep);

		void toSocket(Socket &socket);

		epsg_t epsg;
		std::vector<Point> collection;

		// add a new point
		Point &addPoint(double x, double y);

		// global MetaData (one value per PointCollection)
		const std::string &getGlobalMDString(const std::string &key) const;
		double getGlobalMDValue(const std::string &key) const;
		DirectMetadata<double>* getGlobalMDValueIterator();
		DirectMetadata<std::string>* getGlobalMDStringIterator();
		std::vector<std::string> getGlobalMDValueKeys() const;
		std::vector<std::string> getGlobalMDStringKeys() const;
		void setGlobalMDString(const std::string &key, const std::string &value);
		void setGlobalMDValue(const std::string &key, double value);

		// Export
		std::string toGeoJSON(bool displayMetadata = false);
		std::string toCSV();

		// global MetaData (one value per PointCollection)
		DirectMetadata<std::string> global_md_string;
		DirectMetadata<double> global_md_value;

		// local MetaData (one value per Point)
		MetadataArrays<std::string> local_md_string;
		MetadataArrays<double> local_md_value;
};

#endif
