#ifndef RASTER_GEOMETRY_H
#define RASTER_GEOMETRY_H

#include "raster/raster.h" // epsg_t

#include <geos/geom/Geometry.h>
#include <string>


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

		void setGeom(geos::geom::Geometry *new_geom);
		std::string toWKT();
		std::string toGeoJSON();
	private:
		epsg_t epsg;
		geos::geom::Geometry *geom;
};


#endif
