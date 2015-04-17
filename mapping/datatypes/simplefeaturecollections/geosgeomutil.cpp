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

//construct MultiPolygonCollection as we currently need it for GFBioWS,
//i.e. each polygon in geos multipolygon becomes one elements in a MAPPING MultiPolygonCollection
std::unique_ptr<MultiPolygonCollection> GeosGeomUtil::createMultiPolygonCollection(const geos::geom::MultiPolygon& multiPolygon){

	std::unique_ptr<MultiPolygonCollection> multiPolygonCollection = std::make_unique<MultiPolygonCollection>(SpatioTemporalReference(resolveGeosSRID(multiPolygon.getSRID()), timetype_t::TIMETYPE_UNKNOWN));

	for(size_t polygonIndex = 0; polygonIndex < multiPolygon.getNumGeometries(); ++polygonIndex){
		addPolygon(*multiPolygonCollection, *multiPolygon.getGeometryN(polygonIndex), true);
	}

	return multiPolygonCollection;
}


/**
 * Append a new polygon to the MultiPolygonCollection. If newFeature is true then a new element (=multipolygon) is started
 * otherwise it as added to the last multipolygon
 */
void GeosGeomUtil::addPolygon(MultiPolygonCollection& multiPolygonCollection, const geos::geom::Geometry& polygonGeometry, bool newFeature){
	if(polygonGeometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_POLYGON){
		throw ConverterException("GEOS Geometry is not a Polygon");
	}

	const geos::geom::Polygon& polygon = dynamic_cast<const geos::geom::Polygon&> (polygonGeometry);

	auto& startRing = multiPolygonCollection.start_ring;
	auto& startPolygon = multiPolygonCollection.start_polygon;
	auto& startFeature = multiPolygonCollection.start_feature;
	auto& points = multiPolygonCollection.coordinates;

	//add new polygon
	if(newFeature) {
		//(=feature in this case)
		startFeature.push_back(startPolygon.size());
	}

	startPolygon.push_back(startRing.size());

	//outer ring
	auto outerRing = polygon.getExteriorRing()->getCoordinates();
	startRing.push_back(points.size());
	for(size_t i = 0; i < outerRing->getSize(); ++i){
		points.push_back(Coordinate(outerRing->getX(i), outerRing->getY(i)));
	}

	//inner rings
	for(size_t innerRingIndex = 0; innerRingIndex < polygon.getNumInteriorRing(); ++innerRingIndex){
		auto innerRing = polygon.getInteriorRingN(innerRingIndex)->getCoordinates();
		startRing.push_back(points.size());

		for(size_t i = 0; i < innerRing->getSize(); ++i){
			points.push_back(Coordinate(innerRing->getX(i), innerRing->getY(i)));
		}
	}
}

std::unique_ptr<MultiPolygonCollection> GeosGeomUtil::createMultiPolygonCollection(const geos::geom::Geometry& geometry){
	if(geometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	std::unique_ptr<MultiPolygonCollection> multiPolygonCollection = std::make_unique<MultiPolygonCollection>(SpatioTemporalReference(resolveGeosSRID(geometry.getSRID()), timetype_t::TIMETYPE_UNKNOWN));

	for(size_t i=0; i  < geometry.getNumGeometries(); ++i){

		if(geometry.getGeometryN(i)->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POLYGON){
			addPolygon(*multiPolygonCollection, *geometry.getGeometryN(i), true);
		}
		else if (geometry.getGeometryN(i)->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON){
			const geos::geom::Geometry& multiPolygon = *geometry.getGeometryN(i);
			bool first = true;
			for(size_t polygonIndex = 0; polygonIndex < multiPolygon.getNumGeometries(); ++polygonIndex){
				addPolygon(*multiPolygonCollection, *multiPolygon.getGeometryN(polygonIndex), first);
				first = false;
			}
		}
		else {
			throw ConverterException("GEOS GeometryCollection contains non polygon element");
		}

	}

	return multiPolygonCollection;
}

//for now always create collection of multipolygons
std::unique_ptr<geos::geom::Geometry> GeosGeomUtil::createGeosGeometry(const MultiPolygonCollection& multiPolygonCollection){

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();

	//TODO: calculate size beforehand
	std::unique_ptr<std::vector<geos::geom::Geometry*>> multiPolygons(new std::vector<geos::geom::Geometry*>);

	for(size_t featureIndex = 0; featureIndex < multiPolygonCollection.start_feature.size(); ++featureIndex){
		std::unique_ptr<std::vector<geos::geom::Geometry*>> polygons(new std::vector<geos::geom::Geometry*>);

		for(size_t polygonIndex = multiPolygonCollection.start_feature[featureIndex]; polygonIndex < multiPolygonCollection.stopFeature(featureIndex); ++polygonIndex){

			//outer ring
			size_t ringIndex = multiPolygonCollection.start_polygon[polygonIndex];

			//TODO: calculate size beforehand
			std::unique_ptr<std::vector<geos::geom::Coordinate>> coordinates (new std::vector<geos::geom::Coordinate>);

			//outer ring
			for(size_t pointsIndex = multiPolygonCollection.start_ring[ringIndex]; pointsIndex < multiPolygonCollection.stopRing(ringIndex); ++pointsIndex){
				const Coordinate& point = multiPolygonCollection.coordinates[pointsIndex];
				coordinates->push_back(geos::geom::Coordinate(point.x, point.y));
			}

			const geos::geom::CoordinateSequenceFactory* csf = geos::geom::CoordinateArraySequenceFactory::instance();

			geos::geom::CoordinateSequence* coordinateSequence = csf->create(coordinates.release());

			geos::geom::LinearRing* outerRing = gf->createLinearRing(coordinateSequence);


			//inner rings
			//TODO calculate size beforehand
			std::unique_ptr<std::vector<geos::geom::Geometry*>> innerRings (new std::vector<geos::geom::Geometry*>);

			for(++ringIndex; ringIndex < multiPolygonCollection.stopPolygon(ringIndex); ++ringIndex){
				coordinates.reset(new std::vector<geos::geom::Coordinate>);

				for(size_t pointsIndex = multiPolygonCollection.start_ring[ringIndex]; pointsIndex < multiPolygonCollection.stopRing(ringIndex); ++pointsIndex){
					const Coordinate& point = multiPolygonCollection.coordinates[pointsIndex];
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
	collection->setSRID(resolveMappingEPSG(multiPolygonCollection.stref.epsg));

	return collection;
}
