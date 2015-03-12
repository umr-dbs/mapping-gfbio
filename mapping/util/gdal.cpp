

#include "datatypes/raster.h"
#include "util/gdal.h"

#include <stdint.h>
#include <cstdlib>
#include <mutex>
#include <sstream>

#include <gdal_priv.h>
#include <gdal_alg.h>

#include <cpl_string.h>
#include <ogr_spatialref.h>


namespace GDAL {


static std::once_flag gdal_init_once;

void init() {
	std::call_once(gdal_init_once, []{
		GDALAllRegister();
		//GetGDALDriverManager()->AutoLoadDrivers();
	});
}


std::string SRSFromEPSG(epsg_t epsg) {
		std::ostringstream epsg_name;
		epsg_name << "EPSG:" << (int) epsg;

		std::string epsg_name_str = epsg_name.str();
		OGRSpatialReferenceH hSRS;
		char *pszResult = NULL;

		CPLErrorReset();

		hSRS = OSRNewSpatialReference( NULL );
		bool success = false;

		if(epsg == EPSG_GEOSMSG) {
			//MSG handling
			success = (OSRSetGEOS(hSRS, 0, 35785831, 0, 0) == OGRERR_NONE); //this is valid for meteosat: lon, height, easting, northing (gdal notation)!
			success = (OSRSetWellKnownGeogCS(hSRS, "WGS84" ) == OGRERR_NONE);
			/*
			 * GDAL also uses the following lines:
			 *     oSRS.SetGeogCS( NULL, NULL, NULL, 6378169, 295.488065897, NULL, 0, NULL, 0 );
    		 *     oSRS.SetGeogCS( "unnamed ellipse", "unknown", "unnamed", 6378169, 295.488065897, "Greenwich", 0.0);
			 */
		}
		else {
			//others
			success = (OSRSetFromUserInput( hSRS, epsg_name_str.c_str() ) == OGRERR_NONE );
		}

		if(success)
			//check for success and throw an exception if something went wrong
			OSRExportToWkt( hSRS, &pszResult );
		else {
			std::ostringstream msg;
			msg << "SRS could not be created for epsg " << (int) epsg;
			throw GDALException(msg.str());
			/*
				CPLError( CE_Failure, CPLE_AppDefined,
									"Translating source or target SRS failed:\n%s",
									pszUserInput );
				*/
		}

		OSRDestroySpatialReference(hSRS);

		std::string result(pszResult);
		CPLFree(pszResult);
		return result;
}



CRSTransformer::CRSTransformer(epsg_t in_epsg, epsg_t out_epsg) : in_epsg(in_epsg), out_epsg(out_epsg), transformer(nullptr) {
	if (in_epsg == EPSG_UNKNOWN || out_epsg == EPSG_UNKNOWN)
		throw GDALException("in- or out-epsg is UNKNOWN");
	if (in_epsg == out_epsg)
		throw GDALException("Cannot transform when in_epsg == out_epsg");

	transformer = GDALCreateReprojectionTransformer(SRSFromEPSG(in_epsg).c_str(), SRSFromEPSG(out_epsg).c_str());
	if (!transformer)
		throw GDALException("Could not initialize ReprojectionTransformer");
}

CRSTransformer::~CRSTransformer() {
	if (transformer) {
		GDALDestroyReprojectionTransformer(transformer);
		transformer = nullptr;
	}
}

bool CRSTransformer::transform(double &px, double &py, double &pz) const {
	int success;
	if (in_epsg != out_epsg) {
		if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success)
			return false;
	}

	return true;
}


} // End namespace GDAL
