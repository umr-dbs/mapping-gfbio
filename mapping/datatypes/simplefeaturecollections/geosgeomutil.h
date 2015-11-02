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
	 * add a feature as geos Geometry to a PointCollection
	 * @param collection the collection
	 * @param Geometry the geos Geometry (Point/MultiPoint)
	 */
	static void addFeatureToCollection(PointCollection& pointCollection, const geos::geom::Geometry& geometry);

	/**
	 * add a feature as geos Geometry to a LineCollection
	 * @param collection the collection
	 * @param Geometry the geos Geometry (Line/MultiLine)
	 */
	static void addFeatureToCollection(LineCollection& LineCollection, const geos::geom::Geometry& geometry);

	/**
	 * add a feature as geos Geometry to a PolygonCollection
	 * @param collection the collection
	 * @param Geometry the geos Geometry (Polygon/MultiPolygon)
	 */
	static void addFeatureToCollection(PolygonCollection& polygonCollection, const geos::geom::Geometry& geometry);

	/**
	 * Convert a geos geometry(collection) into a PointCollection
	 * @param geometry the geometry collection
	 * @return the PointCollection converted from geos Geometry
	 */
	static std::unique_ptr<PointCollection> createPointCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a PointCollection into a geos Geometry
	 * @param pointCollection the PointCollection
	 * @return the geos Geometry Collection converted from PointCollection
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosPointCollection(const PointCollection& pointCollection);

	/**
	 * Convert a geos geometry(collection) into a LineCollection
	 * @param geometry the geos Geometry
	 * @return LineCollection converted from geos Geometry
	 */
	static std::unique_ptr<LineCollection> createLineCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a LineCollection into a geos Geometry
	 * @param lineCollection the LineCollection
	 * @return geos Geometry collection converted from LineCollection
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosLineCollection(const LineCollection& lineCollection);


	/**
	 * Convert a geos multipolygon into a PolygonCollection
	 * @param multiPolygon the geos MultiPolygon
	 * @return the PolygonCollection converted from geos MultiPolygon
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::MultiPolygon& multiPolygon);

	/**
	 * Convert a geos geometry(collection) into a PolygonCollection of polygons
	 * @param geometry the geos Geometry
	 * @return the PolygonCollection converted from geos Geometry
	 */
	static std::unique_ptr<PolygonCollection> createPolygonCollection(const geos::geom::Geometry& geometry);

	/**
	 * Convert a PolygonCollection into a geos Geometry
	 * @param polygonCollection
	 * @return the geos Geometry converted from PolygonCollection
	 */
	static std::unique_ptr<geos::geom::Geometry> createGeosPolygonCollection(const PolygonCollection& polygonCollection);


	virtual ~GeosGeomUtil();

private:
	static void addPolygon(PolygonCollection& polygonCollection, const geos::geom::Geometry& polygonGeometry);

	static epsg_t resolveGeosSRID(int srid);
	static int resolveMappingEPSG(epsg_t epsg);
};

#endif
