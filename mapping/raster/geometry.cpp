

#include "raster/geometry.h"

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


static void csToGeoJSON(geos::geom::CoordinateSequence *sequence, std::ostringstream &output, size_t maxpoints = 2147483647) {
	auto size = sequence->getSize();
	for (size_t i=0;i<size && i < maxpoints;i++) {
		if (i > 0)
			output << ",";
		const geos::geom::Coordinate &c = sequence->getAt(i);
		output << "[" << c.x << ", " << c.y << "]";
	}
}

static void polygonToGeoJSON(const geos::geom::Polygon *polygon, std::ostringstream &output) {
	output << "[[";
	std::unique_ptr<geos::geom::CoordinateSequence> exterior(polygon->getExteriorRing()->getCoordinates());
	csToGeoJSON(exterior.get(), output);
	output << "]";
	auto length = polygon->getNumInteriorRing();
	for (size_t i = 0;i<length;i++) {
		std::unique_ptr<geos::geom::CoordinateSequence> interior(polygon->getInteriorRingN(i)->getCoordinates());
		output << ",[";
		csToGeoJSON(interior.get(), output);
		output << "]";
	}
	output << "]";
}


static void geomToGeoJSON(const geos::geom::Geometry *geom, std::ostringstream &output) {
	std::unique_ptr<geos::geom::CoordinateSequence> coords(geom->getCoordinates());
	switch (geom->getGeometryTypeId()) {
		// geometry is a single coordinate
		case geos::geom::GeometryTypeId::GEOS_POINT:
			output << "{ \"type\": \"Point\", \"coordinates\": ";
			csToGeoJSON(coords.get(), output, 1);
			output << "}";
			break;
		// geometry is an array of coordinates
		case geos::geom::GeometryTypeId::GEOS_LINESTRING:
		case geos::geom::GeometryTypeId::GEOS_LINEARRING:
			output << "{ \"type\": \"LineString\", \"coordinates\": [";
			csToGeoJSON(coords.get(), output);
			output << "]}";
			break;
		case geos::geom::GeometryTypeId::GEOS_MULTIPOINT:
			output << "{ \"type\": \"MultiPoint\", \"coordinates\": [";
			csToGeoJSON(coords.get(), output);
			output << "]}";
			break;
		// geometry is an array of arrays of coordinates
		case geos::geom::GeometryTypeId::GEOS_POLYGON: {
			const geos::geom::Polygon *polygon = dynamic_cast<const geos::geom::Polygon *>(geom);
			output << "{ \"type\": \"Polygon\", \"coordinates\": ";
			polygonToGeoJSON(polygon, output);
			output << "}";
			break;
		}
		case geos::geom::GeometryTypeId::GEOS_MULTILINESTRING:
			throw ArgumentException("Cannot (yet) convert a geometry of type GEOS_MULTILINESTRING to GeoJSON");
			break;
		// geometry is a multi-dimensional array
		case geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON: {
			output << "{ \"type\": \"MultiPolygon\", \"coordinates\": [";
			auto length = geom->getNumGeometries();
			for (size_t i = 0;i<length;i++) {
				if (i > 0)
					output << ",\n";
				const geos::geom::Polygon *polygon = dynamic_cast<const geos::geom::Polygon *>(geom->getGeometryN(i));
				polygonToGeoJSON(polygon, output);
			}
			output << "]}";
			break;
		}
		// no geometry, just more geometries
		case geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION: {
			output << "{ \"type\": \"GeometryCollection\", \"geometries\": [";
			auto length = geom->getNumGeometries();
			for (size_t i = 0;i<length;i++) {
				if (i > 0)
					output << ",\n";
				geomToGeoJSON(geom->getGeometryN(i), output);
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
	json << "{\"type\":\"Feature\",\"crs\": {\"type\": \"name\", \"properties\":{\"name\": \"EPSG:" << epsg <<"\"}},\"geometry\":";
	geomToGeoJSON(geom, json);
	json << "}";

	return json.str();
}
