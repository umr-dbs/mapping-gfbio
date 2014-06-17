#ifndef RASTER_POINTCOLLECTION_H
#define RASTER_POINTCOLLECTION_H

#include "raster/raster.h"
#include "raster/metadata.h"
#include <vector>
#include <map>
#include <string>
#include <sys/types.h>


class Point {
	private:
		Point(double x, double y, uint16_t size_md_string = 0, uint16_t size_md_value = 0);
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
	private:
		IndexedMetadata<std::string> md_string;
		IndexedMetadata<double> md_value;
		friend class PointCollection;
};


class PointCollection {
	public:
		PointCollection(epsg_t epsg = EPSG_UNKNOWN);
		~PointCollection();
		epsg_t epsg;
		std::vector<Point> collection;

		// add a new point
		Point &addPoint(double x, double y);

		// global MetaData (stored on the PointCollection)
		const std::string &getGlobalMDString(const std::string &key);
		double getGlobalMDValue(const std::string &key);

		void setGlobalMDString(const std::string &key, const std::string &value);
		void setGlobalMDValue(const std::string &key, double value);


		// local MetaData (metadata stored on the Points themselves)
		// The collection just keeps a list of the allowed keys and their index.
		void addLocalMDString(const std::string &key);
		void addLocalMDValue(const std::string &key);


		// local MetaData (stored on the Points)
		const std::string &getLocalMDString(const Point &point, const std::string &key);
		double getLocalMDValue(const Point &point, const std::string &key);
		void setLocalMDString(Point &point, const std::string &key, const std::string &value);
		void setLocalMDValue(Point &point, const std::string &key, double value);

		// Export
		std::string toGeoJSON();
	private:
		DirectMetadata<std::string> global_md_string;
		DirectMetadata<double> global_md_value;
		MetadataIndex<std::string> local_md_string;
		MetadataIndex<double> local_md_value;
};

#endif
