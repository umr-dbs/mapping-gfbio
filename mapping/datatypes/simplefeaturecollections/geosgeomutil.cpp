#include "geosgeomutil.h"
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/MultiLineString.h>
#include "util/make_unique.h"
#include "util/exceptions.h"

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


void GeosGeomUtil::addFeatureToCollection(PointCollection& pointCollection, const geos::geom::Geometry& geometry){
	if(geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POINT){
		auto& coordinate = *geometry.getCoordinate();
		pointCollection.addSinglePointFeature(Coordinate(coordinate.x, coordinate.y));
	}
	else if (geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOINT){
		const geos::geom::Geometry& multiPoint = geometry;
		for(size_t pointIndex = 0; pointIndex < multiPoint.getNumGeometries(); ++pointIndex){
			auto& coordinate = *multiPoint.getGeometryN(pointIndex)->getCoordinate();
			pointCollection.addCoordinate(coordinate.x, coordinate.y);
		}
		pointCollection.finishFeature();
	}
	else {
		throw ConverterException("GEOS GeometryCollection contains non point element");
	}
}

std::unique_ptr<PointCollection> GeosGeomUtil::createPointCollection(const geos::geom::Geometry& geometry, const SpatioTemporalReference& stref){
	if(geometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	std::unique_ptr<PointCollection> pointCollection = make_unique<PointCollection>(stref);

	for(size_t i=0; i  < geometry.getNumGeometries(); ++i){
		addFeatureToCollection(*pointCollection, *geometry.getGeometryN(i));
	}

	return pointCollection;
}

std::unique_ptr<geos::geom::Geometry> GeosGeomUtil::createGeosPointCollection(const PointCollection& pointCollection){
	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();

	std::unique_ptr<std::vector<geos::geom::Geometry*>> geometries(new std::vector<geos::geom::Geometry*>);

	std::vector<geos::geom::Coordinate> coordinates;
	for(auto feature : pointCollection){


		for(auto& coordinate : feature){
			coordinates.push_back(geos::geom::Coordinate(coordinate.x, coordinate.y));
		}
		geometries->push_back(gf->createMultiPoint(coordinates));
	}

	std::unique_ptr<geos::geom::Geometry> points (gf->createGeometryCollection(geometries.release()));

	return points;
}

void GeosGeomUtil::addFeatureToCollection(LineCollection& lineCollection, const geos::geom::Geometry& geometry){

	if(geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_LINESTRING){
		const geos::geom::LineString& lineString = dynamic_cast<const geos::geom::LineString&>(geometry);
		const auto& coordinates = lineString.getCoordinatesRO();
		for(size_t coordinateIndex = 0; coordinateIndex < coordinates->size(); ++coordinateIndex){
			lineCollection.addCoordinate(coordinates->getX(coordinateIndex), coordinates->getY(coordinateIndex));
		}
		lineCollection.finishLine();
		lineCollection.finishFeature();
	}
	else if (geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTILINESTRING){
		const geos::geom::Geometry& multiLineString = geometry;

		for(size_t lineIndex = 0; lineIndex < multiLineString.getNumGeometries(); ++lineIndex){
			const geos::geom::LineString& lineString = dynamic_cast<const geos::geom::LineString&>(*multiLineString.getGeometryN(lineIndex));
			const auto& coordinates = lineString.getCoordinatesRO();
			for(size_t coordinateIndex = 0; coordinateIndex < coordinates->size(); ++coordinateIndex){
				lineCollection.addCoordinate(coordinates->getX(coordinateIndex), coordinates->getY(coordinateIndex));
			}
			lineCollection.finishLine();
		}
		lineCollection.finishFeature();
	}
	else {
		throw ConverterException("GEOS GeometryCollection contains non line element");
	}
}

std::unique_ptr<LineCollection> GeosGeomUtil::createLineCollection(const geos::geom::Geometry& geometry, const SpatioTemporalReference& stref){
	if(geometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	std::unique_ptr<LineCollection> lineCollection = make_unique<LineCollection>(stref);

	for(size_t i=0; i < geometry.getNumGeometries(); ++i){
		addFeatureToCollection(*lineCollection, *geometry.getGeometryN(i));
	}

	return lineCollection;
}

std::unique_ptr<geos::geom::Geometry> GeosGeomUtil::createGeosLineCollection(const LineCollection& lineCollection){

	const geos::geom::GeometryFactory *gf = geos::geom::GeometryFactory::getDefaultInstance();

	std::unique_ptr<std::vector<geos::geom::Geometry*>> multiLines(new std::vector<geos::geom::Geometry*>);

	for(size_t featureIndex = 0; featureIndex < lineCollection.getFeatureCount(); ++featureIndex){
		std::unique_ptr<std::vector<geos::geom::Geometry*>> lines(new std::vector<geos::geom::Geometry*>);

		for(size_t lineIndex = lineCollection.start_feature[featureIndex]; lineIndex < lineCollection.start_feature[featureIndex + 1]; ++lineIndex){

			std::unique_ptr<std::vector<geos::geom::Coordinate>> coordinates (new std::vector<geos::geom::Coordinate>);
			for(size_t pointsIndex = lineCollection.start_line[lineIndex]; pointsIndex < lineCollection.start_line[lineIndex + 1]; ++pointsIndex){
				const Coordinate& point = lineCollection.coordinates[pointsIndex];
				coordinates->push_back(geos::geom::Coordinate(point.x, point.y));
			}

			const geos::geom::CoordinateSequenceFactory* csf = geos::geom::CoordinateArraySequenceFactory::instance();
			geos::geom::CoordinateSequence* coordinateSequence = csf->create(coordinates.release());

			lines->push_back(gf->createLineString(coordinateSequence));
		}

		multiLines->push_back(gf->createMultiLineString(lines.release()));
	}

	std::unique_ptr<geos::geom::Geometry> collection (gf->createGeometryCollection(multiLines.release()));

	//do this beforehand?
	collection->setSRID(resolveMappingEPSG(lineCollection.stref.epsg));

	return collection;
}

//construct PolygonCollection as we currently need it for GFBioWS,
//i.e. each polygon in geos multipolygon becomes one elements in a MAPPING PolygonCollection
std::unique_ptr<PolygonCollection> GeosGeomUtil::createPolygonCollection(const geos::geom::MultiPolygon& multiPolygon, const SpatioTemporalReference& stref){

	std::unique_ptr<PolygonCollection> polygonCollection = make_unique<PolygonCollection>(stref);

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
	auto outerRing = polygon.getExteriorRing()->getCoordinatesRO();
	for(size_t i = 0; i < outerRing->getSize(); ++i){
		polygonCollection.addCoordinate(outerRing->getX(i), outerRing->getY(i));
	}
	polygonCollection.finishRing();

	//inner rings
	for(size_t innerRingIndex = 0; innerRingIndex < polygon.getNumInteriorRing(); ++innerRingIndex){
		auto innerRing = polygon.getInteriorRingN(innerRingIndex)->getCoordinatesRO();

		for(size_t i = 0; i < innerRing->getSize(); ++i){
			polygonCollection.addCoordinate(innerRing->getX(i), innerRing->getY(i));
		}
		polygonCollection.finishRing();
	}

	polygonCollection.finishPolygon();
}

void GeosGeomUtil::addFeatureToCollection(PolygonCollection& polygonCollection, const geos::geom::Geometry& geometry){
	if(geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POLYGON){
		addPolygon(polygonCollection, geometry);
		polygonCollection.finishFeature();
	}
	else if (geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON){
		const geos::geom::Geometry& multiPolygon = geometry;
		for(size_t polygonIndex = 0; polygonIndex < multiPolygon.getNumGeometries(); ++polygonIndex){
			addPolygon(polygonCollection, *multiPolygon.getGeometryN(polygonIndex));
		}
		polygonCollection.finishFeature();
	}
	else {
		throw ConverterException("GEOS GeometryCollection contains non polygon element");
	}
}

std::unique_ptr<PolygonCollection> GeosGeomUtil::createPolygonCollection(const geos::geom::Geometry& geometry, const SpatioTemporalReference& stref){
	if(geometry.getGeometryTypeId() != geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION){
		throw ConverterException("GEOS Geometry is not a geometry collection");
	}

	std::unique_ptr<PolygonCollection> polygonCollection = make_unique<PolygonCollection>(stref);

	for(size_t i=0; i  < geometry.getNumGeometries(); ++i){
		addFeatureToCollection(*polygonCollection, *geometry.getGeometryN(i));
	}

	return polygonCollection;
}

//for now always create collection of multipolygons
std::unique_ptr<geos::geom::Geometry> GeosGeomUtil::createGeosPolygonCollection(const PolygonCollection& polygonCollection){

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
