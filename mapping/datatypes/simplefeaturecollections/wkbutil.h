#ifndef UTIL_WKBPARSER_H_
#define UTIL_WKBPARSER_H_

#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include <memory>

/**
 * Utility class to read/write WKB to/from collection instances
 */
class WKBUtil {
public:
	WKBUtil() = delete;

	/**
	 * read PointCollection from well-known binary
	 * @param wkb the well-known binary containing a collection of Points/Multi-Points
	 * @return PointCollection from well-known binary
	 */
	static std::unique_ptr<PointCollection> readPointCollection(std::stringstream& wkb);

	/**
	 * read LineCollection from well-known binary
	 * @param wkb the well-known binary containing a collection of Lines/Multi-Lines
	 * @return LineCollection from well-known binary
	 */
	static std::unique_ptr<LineCollection> readLineCollection(std::stringstream& wkb);

	/**
	 * read PolygonCollection from well-known binary
	 * @param wkb the well-known binary containing a collection of Polygon/Multi-Polygons
	 * @return PolygonCollection from well-known binary
	 */
	static std::unique_ptr<PolygonCollection> readPolygonCollection(std::stringstream& wkb);

	/**
	 * read PointCollection from well-known text
	 * @param wkt the well-known text containing a collection of Points/Multi-Points
	 * @return PointCollection from well-known text
	 */
	static std::unique_ptr<PointCollection> readPointCollection(std::string& wkt);

	/**
	 * read LineCollection from well-known text
	 * @param wkt the well-known text containing a collection of Lines/Multi-Lines
	 * @return LineCollection from well-known text
	 */
	static std::unique_ptr<LineCollection> readLineCollection(std::string& wkt);

	/**
	 * read PolygonCollection from well-known text
	 * @param wkt the well-known text containing a collection of Polygon/Multi-Polygons
	 * @return PolygonCollection from well-known text
	 */
	static std::unique_ptr<PolygonCollection> readPolygonCollection(std::string& wkt);


	/**
	 * add a feature as well-known text to a PointCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(PointCollection& collection, std::string& wkt);

	/**
	 * add a feature as well-known text to a LineCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(LineCollection& collection, std::string& wkt);

	/**
	 * add a feature as well-known text to a PolygonCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(PolygonCollection& collection, std::string& wkt);

};

#endif
