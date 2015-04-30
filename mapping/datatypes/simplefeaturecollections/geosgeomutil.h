#ifndef DATATYPES_COLLECTIONS_GEOSGEOMUTIL_H_
#define DATATYPES_COLLECTIONS_GEOSGEOMUTIL_H_
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include <geos/geom/MultiPolygon.h>

/**
 * Utility class to convert Mapping geometries to geos geometries and vice versa
 */
class GeosGeomUtil {
public:
	GeosGeomUtil() = delete;

	/**
	 * Convert a geos geometry(collection) into a PointCollectio
	 */
	static std::unique_ptr<PointCollection> createPointCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a PointCollection into a geos Geometry
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosPointCollection(const PointCollection& pointCollection);

	/**
	 * Convert a geos geometry(collection) into a LineCollection
	 */
	static std::unique_ptr<LineCollection> createLineCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a LineCollection into a geos Geometry
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosLineCollection(const LineCollection& lineCollection);


	/**
	 * Convert a geos multipolygon into a PolygonCollection
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::MultiPolygon& multiPolygon);

	/**
	 * Convert a geos geometry(collection) into a PolygonCollection of polygons
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a PolygonCollection into a geos Geometry
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosPolygonCollection(const PolygonCollection& polygonCollection);


	virtual ~GeosGeomUtil();

private:
	static void addPolygon(PolygonCollection& polygonCollection, const geos::geom::Geometry& polygonGeometry);

	static epsg_t resolveGeosSRID(int srid);
	static int resolveMappingEPSG(epsg_t epsg);
};

#endif
