

#include "datatypes/geometry.h"

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LineString.h>
#include <geos/io/WKTWriter.h>

#include <sstream>


GenericGeometry::GenericGeometry(epsg_t epsg) : epsg(epsg), geom(nullptr) {

}
GenericGeometry::~GenericGeometry() {
	setGeom(nullptr);
}

void GenericGeometry::setGeom(geos::geom::Geometry *new_geom) {
	if (geom) {
		auto factory = geom->getFactory();
		factory->destroyGeometry(geom);
	}
	geom = new_geom;
}

std::string GenericGeometry::toWKT() {
	geos::io::WKTWriter wktwriter;

	return wktwriter.write(geom);
}

static void geomToGeoJSONCoordinates(const geos::geom::Geometry *geom, std::ostringstream &output, int depth) {
	output << "[";
	if (depth > 0) {
		if (geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POLYGON) {
			const geos::geom::Polygon *polygon = dynamic_cast<const geos::geom::Polygon *>(geom);
			const geos::geom::Geometry *exterior = polygon->getExteriorRing();
			geomToGeoJSONCoordinates(exterior, output, depth-1);
			auto length = polygon->getNumInteriorRing();
			for (size_t i = 0;i<length;i++) {
				output << ",";
				const geos::geom::Geometry *interior = polygon->getInteriorRingN(i);
				geomToGeoJSONCoordinates(interior, output, depth-1);
			}
		}
		else {
			auto length = geom->getNumGeometries();
			for (size_t i = 0;i<length;i++) {
				if (i > 0)
					output << ",\n";
				const geos::geom::Geometry *child = geom->getGeometryN(i);
				geomToGeoJSONCoordinates(child, output, depth-1);
			}
		}
	}
	else {
		std::unique_ptr<geos::geom::CoordinateSequence> sequence(geom->getCoordinates());
		auto size = sequence->getSize();
		if (size == 0)
			throw ArgumentException("Cannot encode Geometry with empty coordinate lists");
		if (geom->getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POINT) {
			if (size != 1)
				throw ArgumentException("Cannot encode Point Geometry with more than one set of coordinates");
			const geos::geom::Coordinate &c = sequence->getAt(0);
			output << c.x << ", " << c.y;
		}
		else {
			for (size_t i=0;i<size;i++) {
				if (i > 0)
					output << ",";
				const geos::geom::Coordinate &c = sequence->getAt(i);
				output << "[" << c.x << ", " << c.y << "]";
			}
		}
	}
	output << "]";
}


static void geomToGeoJSONGeometry(const geos::geom::Geometry *geom, std::ostringstream &output) {
	switch (geom->getGeometryTypeId()) {
		// geometry is a single coordinate
		case geos::geom::GeometryTypeId::GEOS_POINT:
			output << "{ \"type\": \"Point\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 0);
			output << "}";
			break;
		// geometry is an array of coordinates
		case geos::geom::GeometryTypeId::GEOS_LINESTRING:
		case geos::geom::GeometryTypeId::GEOS_LINEARRING:
			output << "{ \"type\": \"LineString\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 0);
			output << "}";
			break;
		case geos::geom::GeometryTypeId::GEOS_MULTIPOINT:
			output << "{ \"type\": \"MultiPoint\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 0);
			output << "}";
			break;
		// geometry is an array of arrays of coordinates
		case geos::geom::GeometryTypeId::GEOS_POLYGON:
			output << "{ \"type\": \"Polygon\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 1);
			output << "}";
			break;
		case geos::geom::GeometryTypeId::GEOS_MULTILINESTRING:
			output << "{ \"type\": \"MultiLineString\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 1);
			output << "}";
			break;
		// geometry is a multi-dimensional array
		case geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON:
			output << "{ \"type\": \"MultiPolygon\", \"coordinates\": ";
			geomToGeoJSONCoordinates(geom, output, 2);
			output << "}";
			break;
		// no geometry, just more geometries
		case geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION: {
			output << "{ \"type\": \"GeometryCollection\", \"geometries\": [";
			auto length = geom->getNumGeometries();
			for (size_t i = 0;i<length;i++) {
				if (i > 0)
					output << ",\n";
				geomToGeoJSONGeometry(geom->getGeometryN(i), output);
			}
			output << "]}";
			break;
		}
		default:
			throw ArgumentException("Unknown GeometryType in toGeoJSON");
	}
}

std::string GenericGeometry::toGeoJSON() {
	std::ostringstream json;
	json << std::fixed; // std::setprecision(4);
	json << "{\"type\":\"Feature\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << epsg <<"\"}},\"properties\":{},\"geometry\":";
	geomToGeoJSONGeometry(geom, json);
	json << "}";

	return json.str();
}
