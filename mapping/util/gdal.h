#ifndef UTIL_GDAL_H
#define UTIL_GDAL_H

#include <string>
#include <stdint.h>
#include "datatypes/spatiotemporal.h"

namespace GDAL {
	void init();
	std::string SRSFromEPSG(epsg_t epsg);

	class CRSTransformer {
		public:
			CRSTransformer(epsg_t in_epsg, epsg_t out_epsg);
			~CRSTransformer();

			CRSTransformer(const CRSTransformer &copy) = delete;
			CRSTransformer &operator=(const CRSTransformer &copy) = delete;

			bool transform(double &px, double &py, double &pz) const;
			bool transform(double &px, double &py) const { double pz = 0.0; return transform(px, py, pz); }
			const epsg_t in_epsg, out_epsg;

			static void msg_pixcoord2geocoord(int column, int row, double *longitude, double *latitude);
			static void msg_geocoord2pixcoord(double longitude, double latitude, int *column, int *row);
		private:
			void *transformer;
	};

}

#endif
