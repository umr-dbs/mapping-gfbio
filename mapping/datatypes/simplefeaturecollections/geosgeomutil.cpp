#include "geosgeomutil.h"
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include "util/make_unique.h"
#include "raster/exceptions.h"

//TODO: LineString?


GeosGeomUtil::~GeosGeomUtil() {
}

//TODO: also resolve time reference??
epsg_t GeosGeomUtil::resolveGeosSRID(int srid){
	if(srid == 4326){
		return epsg_t::EPSG_LATLON;
	}
	else if (srid == 3857){
		return epsg_t::EPSG_WEBMERCATOR;
	}

	//TODO: remove workaround that geos doesnt have correct epsg:
	return epsg_t::EPSG_LATLON;
}

int GeosGeomUtil::resolveMappingEPSG(epsg_t epsg){
	switch(epsg){
		case epsg_t::EPSG_LATLON: return 4326;
		case EPSG_WEBMERCATOR: return 3857;
		default: return 0;
	}
}

//construct PolygonCollection as we currently need it for GFBioWS,
//i.e. each polygon in geos multipolygon becomes one elements in a MAPPING PolygonCollection
std::unique_ptr<PolygonCollection> GeosGeomUtil::createPolygonCollection(const geos::geom::MultiPolygon& multiPolygon){

	std::unique_ptr<PolygonCollection> polygonCollection = std::make_unique<PolygonCollection>(SpatioTemporalReference(resolveGeosSRID(multiPolygon.getSRID()), timetype_t::TIMETYPE_UNKNOWN));

	for(size_t polygonIndex = 0; polygonIndex < multiPolygon.getNumGeometries(); ++polygonIndex){
		addPolygon(*polygonCollection, *multiPolygon.getGeometryN(polygonIndex));
		polygonCollection->finishFeature();
	}

	return polygonCollection;
}


/**
 * Append a new polygon to the PolygonCollection
 */
void GeosGeomUtil::addPolygon(PolygonCollection& polygonCollection, const geos::geom::Geometry& polygonGeometry){
	if(polygonGeometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_POLYGON){
		throw ConverterException("GEOS Geometry is not a Polygon");
	}

	const geos::geom::Polygon& polygon = dynamic_cast<const geos::geom::Polygon&> (polygonGeometry);

	auto& startRing = polygonCollection.start_ring;
	auto& startPolygon = polygonCollection.start_polygon;
	auto& startFeature = polygonCollection.start_feature;
	auto& coordinates = polygonCollection.coordinates;

	//outer ring
	auto outerRing = polygon.getExteriorRing()->getCoordinates();
	for(size_t i = 0; i < outerRing->getSize(); ++i){
		polygonCollection.addCoordinate(outerRing->getX(i), outerRing->getY(i));
	}
	polygonCollection.finishRing();

	//inner rings
	for(size_t innerRingIndex = 0; innerRingIndex < polygon.getNumInteriorRing(); ++innerRingIndex){
		auto innerRing = polygon.getInteriorRingN(innerRingIndex)->getCoordinates();

		for(size_t i = 0; i < innerRing->getSize(); ++i){
			polygonCollection.addCoordinate(innerRing->getX(i), innerRing->getY(i));
		}
		polygonCollection.finishRing();
	}

	polygonCollection.finishPolygon();
}

std::unique_ptr<PolygonCollection> GeosGeomUtil::createPolygonCollection(const geos::geom::Geometry& geometry){
	if(geometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	std::unique_ptr<PolygonCollection> polygonCollection = std::make_unique<PolygonCollection>(SpatioTemporalReference(resolveGeosSRID(geometry.getSRID()), timetype_t::TIMETYPE_UNKNOWN));

	for(size_t i=0; i  < geometry.getNumGeometries(); ++i){

		if(geometry.getGeometryN(i)->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POLYGON){
			addPolygon(*polygonCollection, *geometry.getGeometryN(i));
			polygonCollection->finishFeature();
		}
		else if (geometry.getGeometryN(i)->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON){
			const geos::geom::Geometry& multiPolygon = *geometry.getGeometryN(i);
			for(size_t polygonIndex = 0; polygonIndex < multiPolygon.getNumGeometries(); ++polygonIndex){
				addPolygon(*polygonCollection, *multiPolygon.getGeometryN(polygonIndex));
			}
			polygonCollection->finishFeature();
		}
		else {
			throw ConverterException("GEOS GeometryCollection contains non polygon element");
		}

	}

	return polygonCollection;
}

//for now always create collection of multipolygons
std::unique_ptr<geos::geom::Geometry> GeosGeomUtil::createGeosGeometry(const PolygonCollection& polygonCollection){

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();

	//TODO: calculate size beforehand
	std::unique_ptr<std::vector<geos::geom::Geometry*>> multiPolygons(new std::vector<geos::geom::Geometry*>);

	for(size_t featureIndex = 0; featureIndex < polygonCollection.getFeatureCount(); ++featureIndex){
		std::unique_ptr<std::vector<geos::geom::Geometry*>> polygons(new std::vector<geos::geom::Geometry*>);

		for(size_t polygonIndex = polygonCollection.start_feature[featureIndex]; polygonIndex < polygonCollection.start_feature[featureIndex + 1]; ++polygonIndex){

			//outer ring
			size_t ringIndex = polygonCollection.start_polygon[polygonIndex];

			//TODO: calculate size beforehand
			std::unique_ptr<std::vector<geos::geom::Coordinate>> coordinates (new std::vector<geos::geom::Coordinate>);

			//outer ring
			for(size_t pointsIndex = polygonCollection.start_ring[ringIndex]; pointsIndex < polygonCollection.start_ring[ringIndex + 1]; ++pointsIndex){
				const Coordinate& point = polygonCollection.coordinates[pointsIndex];
				coordinates->push_back(geos::geom::Coordinate(point.x, point.y));
			}

			const geos::geom::CoordinateSequenceFactory* csf = geos::geom::CoordinateArraySequenceFactory::instance();

			geos::geom::CoordinateSequence* coordinateSequence = csf->create(coordinates.release());

			geos::geom::LinearRing* outerRing = gf->createLinearRing(coordinateSequence);


			//inner rings
			//TODO calculate size beforehand
			std::unique_ptr<std::vector<geos::geom::Geometry*>> innerRings (new std::vector<geos::geom::Geometry*>);

			for(++ringIndex; ringIndex < polygonCollection.start_polygon[polygonIndex + 1]; ++ringIndex){
				coordinates.reset(new std::vector<geos::geom::Coordinate>);

				for(size_t pointsIndex = polygonCollection.start_ring[ringIndex]; pointsIndex < polygonCollection.start_ring[ringIndex + 1]; ++pointsIndex){
					const Coordinate& point = polygonCollection.coordinates[pointsIndex];
					coordinates->push_back(geos::geom::Coordinate(point.x, point.y));
				}

				coordinateSequence = csf->create(coordinates.release());
				geos::geom::Geometry* innerRing = gf->createLinearRing(coordinateSequence);

				innerRings->push_back(innerRing);
			}

			polygons->push_back(gf->createPolygon(outerRing, innerRings.release()));
		}

		multiPolygons->push_back(gf->createMultiPolygon(polygons.release()));
	}

	std::unique_ptr<geos::geom::Geometry> collection (gf->createGeometryCollection(multiPolygons.release()));

	//do this beforehand?
	collection->setSRID(resolveMappingEPSG(polygonCollection.stref.epsg));

	return collection;
}
