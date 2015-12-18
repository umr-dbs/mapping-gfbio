#ifndef SERVICES_OGCSERVICE_H
#define SERVICES_OGCSERVICE_H

#include "services/httpservice.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/simplefeaturecollection.h"
#include "datatypes/raster/raster_priv.h"

/*
 * This is an abstract helper class to implement services of the OGC.
 * It contains functionality common to multiple OGC protocols.
 */
class OGCService : public HTTPService {
	protected:
		epsg_t parseEPSG(const Params &params, const std::string &key, epsg_t def = EPSG_WEBMERCATOR);
		double parseTimestamp(const Params &params, double defaultValue = 0);
		void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg = EPSG_WEBMERCATOR, bool allow_infinite = false);

		void outputImage(HTTPResponseStream &stream, GenericRaster *raster, bool flipx = false, bool flipy = false, const std::string &colors = "", Raster2D<uint8_t> *overlay = nullptr);
		void outputSimpleFeatureCollectionGeoJSON(HTTPResponseStream &stream, SimpleFeatureCollection *collection, bool displayMetadata = false);
		void outputSimpleFeatureCollectionCSV(HTTPResponseStream &stream, SimpleFeatureCollection *collection);
		void outputSimpleFeatureCollectionARFF(HTTPResponseStream &stream, SimpleFeatureCollection* collection);
};


#endif
