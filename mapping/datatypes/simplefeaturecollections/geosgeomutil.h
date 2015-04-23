#ifndef DATATYPES_COLLECTIONS_GEOSGEOMUTIL_H_
#define DATATYPES_COLLECTIONS_GEOSGEOMUTIL_H_

#include "datatypes/polygoncollection.h"
#include <geos/geom/MultiPolygon.h>

/**
 * Utility class to convert Mapping geometries to geos geometries and vice versa
 */
class GeosGeomUtil {
public:
	GeosGeomUtil() = delete;

	/**
	 * Convert a geos multipolygon into a PolygonCollection of single polygons
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::MultiPolygon& multiPolygon);


	/**
	 * Convert a geos geometry(collection) into a PolygonCollection of single polygons
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::Geometry& geometry);



	/**
	 * Convert a PolygonCollection into a geos Geometry
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosGeometry(const PolygonCollection& polygonCollection);


	virtual ~GeosGeomUtil();

private:
	static void addPolygon(PolygonCollection& polygonCollection, const geos::geom::Geometry& polygonGeometry);

	static epsg_t resolveGeosSRID(int srid);
	static int resolveMappingEPSG(epsg_t epsg);
};

#endif
