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
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return PointCollection from well-known binary
	 */
	static std::unique_ptr<PointCollection> readPointCollection(std::stringstream& wkb, const SpatioTemporalReference& stref);

	/**
	 * read LineCollection from well-known binary
	 * @param wkb the well-known binary containing a collection of Lines/Multi-Lines
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return LineCollection from well-known binary
	 */
	static std::unique_ptr<LineCollection> readLineCollection(std::stringstream& wkb, const SpatioTemporalReference& stref);

	/**
	 * read PolygonCollection from well-known binary
	 * @param wkb the well-known binary containing a collection of Polygon/Multi-Polygons
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return PolygonCollection from well-known binary
	 */
	static std::unique_ptr<PolygonCollection> readPolygonCollection(std::stringstream& wkb, const SpatioTemporalReference& stref);

	/**
	 * read PointCollection from well-known text
	 * @param wkt the well-known text containing a collection of Points/Multi-Points
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return PointCollection from well-known text
	 */
	static std::unique_ptr<PointCollection> readPointCollection(const std::string& wkt, const SpatioTemporalReference& stref);

	/**
	 * read LineCollection from well-known text
	 * @param wkt the well-known text containing a collection of Lines/Multi-Lines
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return LineCollection from well-known text
	 */
	static std::unique_ptr<LineCollection> readLineCollection(const std::string& wkt, const SpatioTemporalReference& stref);

	/**
	 * read PolygonCollection from well-known text
	 * @param wkt the well-known text containing a collection of Polygon/Multi-Polygons
	 * @param stref the SpatioTemporalReference for the resulting collection
	 * @return PolygonCollection from well-known text
	 */
	static std::unique_ptr<PolygonCollection> readPolygonCollection(const std::string& wkt, const SpatioTemporalReference& stref);


	/**
	 * add a feature as well-known text to a PointCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(PointCollection& collection, const std::string& wkt);

	/**
	 * add a feature as well-known text to a LineCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(LineCollection& collection, const std::string& wkt);

	/**
	 * add a feature as well-known text to a PolygonCollection
	 * @param collection the collection
	 * @param wkt the well-known text
	 */
	static void addFeatureToCollection(PolygonCollection& collection, const std::string& wkt);

};

#endif
