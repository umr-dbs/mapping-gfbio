#ifndef RASTER_GEOMETRY_H
#define RASTER_GEOMETRY_H

#include "raster/raster.h" // epsg_t

#include <geos/geom/Geometry.h>
#include <geos/geom/Coordinate.h>
#include <string>
#include <functional>


class GenericGeometry {
	public:
		GenericGeometry(epsg_t epsg);
		~GenericGeometry();

		GenericGeometry() = delete;
		// Copy
		GenericGeometry(const GenericGeometry &dd) = default;
		GenericGeometry &operator=(const GenericGeometry &dd) = delete;
		// Move
		GenericGeometry(GenericGeometry &&dd) = delete;
		GenericGeometry &operator=(GenericGeometry &&dd) = delete;

		const geos::geom::Geometry *getGeometry() { return geom; }

		void setGeom(geos::geom::Geometry *new_geom);
		std::string toWKT();
		std::string toGeoJSON();

		const epsg_t epsg;
	private:
		geos::geom::Geometry *geom;
};


#endif
