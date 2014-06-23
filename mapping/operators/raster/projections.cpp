
#include "raster/raster.h"
#include "raster/pointcollection.h"
#include "raster/typejuggling.h"
#include "raster/profiler.h"
#include "operators/operator.h"

#include <memory>
#include <sstream>
#include <cmath>
#include <json/json.h>

#ifndef M_PI
#define M_PI    3.14159265358979323846
#endif
#include <gdal_alg.h>

#include <cpl_string.h>
#include <ogr_spatialref.h>




class ProjectionOperator : public GenericOperator {
	public:
		ProjectionOperator(int sourcecount, GenericOperator *sources[], Json::Value params);
		virtual ~ProjectionOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
		virtual PointCollection *getPoints(const QueryRectangle &rect);
	private:
		QueryRectangle projectQueryRectangle(const QueryRectangle &rect, void *transformer);
		GenericRaster *executeFromMeteosat2(int timestamp, double x1, double y1, double x2, double y2, int xres, int yres);
		epsg_t src_epsg, dest_epsg;
};


#if 0
class MeteosatLatLongOperator : public GenericOperator {
	public:
	MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]);
		virtual ~MeteosatLatLongOperator();

		virtual GenericRaster *getRaster(const QueryRectangle &rect);
};
#endif





std::string SanitizeSRS( const char *pszUserInput) {
    OGRSpatialReferenceH hSRS;
    char *pszResult = NULL;

    CPLErrorReset();

    hSRS = OSRNewSpatialReference( NULL );
    if( OSRSetFromUserInput( hSRS, pszUserInput ) == OGRERR_NONE )
        OSRExportToWkt( hSRS, &pszResult );
    else {
    	throw OperatorException("SRS could not be parsed");
    	/*
        CPLError( CE_Failure, CPLE_AppDefined,
                  "Translating source or target SRS failed:\n%s",
                  pszUserInput );
        */
    }

    OSRDestroySpatialReference(hSRS);

    //printf("X-Sanitize: %s -> %s\r\n", pszUserInput, pszResult);
    std::string result(pszResult);
    CPLFree(pszResult);
    return result;
}

std::string SRSFromEPSG(epsg_t epsg) {
    std::ostringstream epsg_name;
    epsg_name << "EPSG:" << epsg;
    return SanitizeSRS(epsg_name.str().c_str());
}




ProjectionOperator::ProjectionOperator(int sourcecount, GenericOperator *sources[], Json::Value params) : GenericOperator(Type::RASTER, sourcecount, sources) {
	src_epsg = params.get("src_epsg", EPSG_UNKNOWN).asInt();
	dest_epsg = params.get("dest_epsg", EPSG_UNKNOWN).asInt();
	if (src_epsg == EPSG_UNKNOWN || dest_epsg == EPSG_UNKNOWN)
		throw OperatorException("Unknown target EPSG");
	assumeSources(1);
}

ProjectionOperator::~ProjectionOperator() {
}
REGISTER_OPERATOR(ProjectionOperator, "projection");

template<typename T>
struct raster_projection {
	static GenericRaster *execute(Raster2D<T> *raster_src, void *transformer, LocalCRS &rm_dest ) {
		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		DataDescription out_dd = raster_src->dd;
		out_dd.addNoData();

		Raster2D<T> *raster_dest = (Raster2D<T> *) GenericRaster::create(rm_dest, out_dd);
		std::unique_ptr<GenericRaster> raster_dest_guard(raster_dest);

		T nodata = (T) out_dd.no_data;

		int width = raster_dest->lcrs.size[0];
		int height = raster_dest->lcrs.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				double px = raster_dest->lcrs.PixelToWorldX(x);
				double py = raster_dest->lcrs.PixelToWorldY(y);
				double pz = 0;

				int success;
				if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success) {
					//printf("failed at (%d,%d): (%f, %f -> %f, %f)\n", x, y, px_bak, py_bak, px, py);
					raster_dest->set(x, y, nodata);
					continue;
				}

				int tx = round(raster_src->lcrs.WorldToPixelX(px));
				int ty = round(raster_src->lcrs.WorldToPixelY(py));
				if (tx >= 0 && ty >= 0 && tx < (int) raster_src->lcrs.size[0] && ty < (int) raster_src->lcrs.size[1]) {
					raster_dest->set(x, y, raster_src->get(tx, ty));
				}
				else
					raster_dest->set(x, y, nodata);
			}
		}

		return raster_dest_guard.release();
	}
};

static void pixcoord2geocoord(int column, int row, double *longitude, double *latitude);
static void geocoord2pixcoord(double longitude, double latitude, int *column, int *row);

template<typename T>
struct raster_projection_from_msat {
	static GenericRaster *execute(Raster2D<T> *raster_src, void *transformer, LocalCRS &rm_dest ) {
		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		DataDescription vm_dest = raster_src->dd;
		vm_dest.addNoData();

		Raster2D<T> *raster_dest = (Raster2D<T> *) GenericRaster::create(rm_dest, vm_dest);
		std::unique_ptr<GenericRaster> raster_dest_guard(raster_dest);

		T nodata = (T) vm_dest.no_data;

		int width = raster_dest->lcrs.size[0];
		int height = raster_dest->lcrs.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				double px = raster_dest->lcrs.PixelToWorldX(x);
				double py = raster_dest->lcrs.PixelToWorldY(y);
				double pz = 0;

				int success;
				if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success) {
					//printf("failed at (%d,%d): (%f, %f -> %f, %f)\n", x, y, px_bak, py_bak, px, py);
					raster_dest->set(x, y, nodata);
					continue;
				}

				// px = longitude, py = latitude
				int tx, ty;
				geocoord2pixcoord(px, py, &tx, &ty);

				tx = round(raster_src->lcrs.WorldToPixelX(tx));
				ty = round(raster_src->lcrs.WorldToPixelY(ty));
				if (tx >= 0 && ty >= 0 && tx < (int) raster_src->lcrs.size[0] && ty < (int) raster_src->lcrs.size[1]) {
					tx = raster_src->lcrs.size[0] - tx;
					raster_dest->set(x, y, raster_src->get(tx, ty));
				}
				else
					raster_dest->set(x, y, nodata);
			}
		}

		return raster_dest_guard.release();
	}
};


QueryRectangle ProjectionOperator::projectQueryRectangle(const QueryRectangle &rect, void *transformer) {
	double src_x1, src_y1, src_x2, src_y2;
	int src_xres = rect.xres, src_yres = rect.yres;

	if (src_epsg == EPSG_METEOSAT2) {
		// Meteosat: ALWAYS load the full raster.
		src_x1 = 0;
		src_y1 = 0;
		src_x2 = 3711;
		src_y2 = 3711;
		src_xres = 3712;
		src_yres = 3712;
	}
	else {
		// Transform the upper left and bottom right corner, use those as the source bounding box
		// That'll only work on transformations where rectangles remain rectangles..
		double px = rect.x1, py = rect.y1, pz=0;
		int success;
		if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success) {
			GDALDestroyReprojectionTransformer(transformer);
			throw OperatorException("Transformation of top left corner failed");
		}
		src_x1 = px;
		src_y1 = py;

		px = rect.x2;
		py = rect.y2;
		if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success) {
			GDALDestroyReprojectionTransformer(transformer);
			throw OperatorException("Transformation of bottom right corner failed");
		}
		src_x2 = px;
		src_y2 = py;

		// TODO: welche Auflösung der Quelle brauchen wir denn überhaupt?
/*
		printf("Content-type: text/plain\r\n\r\n");
		printf("qrect: %f, %f -> %f, %f\n", rect.x1, rect.y1, rect.x2, rect.y2);
		printf("src:   %f, %f -> %f, %f\n", src_x1, src_y1, src_x2, src_y2);
		exit(0);
*/
	}

	return QueryRectangle(rect.timestamp, src_x1, src_y1, src_x2, src_y2, src_xres, src_yres, src_epsg);
}

//GenericRaster *ProjectionOperator::execute(int timestamp, double x1, double y1, double x2, double y2, int xres, int yres) {
GenericRaster *ProjectionOperator::getRaster(const QueryRectangle &rect) {
	if (dest_epsg != rect.epsg) {
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	}
	if (src_epsg == dest_epsg) {
		return sources[0]->getRaster(rect);
	}

	epsg_t transform_source_epsg = src_epsg;
	// Tranforming from MSAT2: we transform to LATLON manually, then let GDAL do the rest.
	if (src_epsg == EPSG_METEOSAT2) {
		transform_source_epsg = EPSG_LATLON;
	}

	if (dest_epsg == EPSG_METEOSAT2)
		throw OperatorException("Cannot transform to Meteosat Projection. Why would you want that?");

	void *transformer = GDALCreateReprojectionTransformer(SRSFromEPSG(dest_epsg).c_str(), SRSFromEPSG(transform_source_epsg).c_str());
	if (!transformer)
		throw OperatorException("Could not initialize ReprojectionTransformer");

	QueryRectangle src_rect = projectQueryRectangle(rect, transformer);

	GenericRaster *raster = sources[0]->getRaster(src_rect);
	std::unique_ptr<GenericRaster> raster_guard(raster);

	if (src_epsg != raster->lcrs.epsg)
		throw OperatorException("ProjectionOperator: Source Raster not in expected projection");

	LocalCRS rm_dest(rect);

	Profiler::Profiler p("PROJECTION_OPERATOR");
	GenericRaster *result_raster = nullptr;
	if (src_epsg == EPSG_METEOSAT2) {
		result_raster = callUnaryOperatorFunc<raster_projection_from_msat>(raster, transformer, rm_dest);
	}
	else {
		result_raster = callUnaryOperatorFunc<raster_projection>(raster, transformer, rm_dest);
	}

	GDALDestroyReprojectionTransformer(transformer);

	return result_raster;
}


PointCollection *ProjectionOperator::getPoints(const QueryRectangle &rect) {
	if (dest_epsg != rect.epsg)
		throw OperatorException("Projection: asked to transform to a different CRS than specified in QueryRectangle");
	if (src_epsg == dest_epsg)
		return sources[0]->getPoints(rect);

	if (src_epsg == EPSG_METEOSAT2 || dest_epsg == EPSG_METEOSAT2)
		throw OperatorException("Cannot transform Points from or to Meteosat Projection. Why would you want that?");

	std::string src_src = SRSFromEPSG(src_epsg);
	std::string dest_srs = SRSFromEPSG(dest_epsg);

	// Need to transform "backwards" to project the query rectangle..
	void *transformer = GDALCreateReprojectionTransformer(dest_srs.c_str(), src_src.c_str());
	if (!transformer)
		throw OperatorException("Could not initialize ReprojectionTransformer#1");

	QueryRectangle src_rect = projectQueryRectangle(rect, transformer);

	// ..but "forward" to project the points
	GDALDestroyReprojectionTransformer(transformer);
	transformer = GDALCreateReprojectionTransformer(src_src.c_str(), dest_srs.c_str());
	if (!transformer)
		throw OperatorException("Could not initialize ReprojectionTransformer#2");


	PointCollection *points_in = sources[0]->getPoints(src_rect);
	std::unique_ptr<PointCollection> points_in_guard(points_in);

	if (src_epsg != points_in->epsg)
		throw OperatorException("ProjectionOperator: Source Points not in expected projection");

	PointCollection *points_out = new PointCollection(dest_epsg);
	std::unique_ptr<PointCollection> points_out_guard(points_out);

	// TODO: copy global metadata
	// TODO: copy local metadata indexes

	//printf("content-type: text/plain\r\n\r\n");
	for (Point &point : points_in->collection) {
		double x = point.x, y = point.y, z = 0.0;
		int success;
		if (!GDALReprojectionTransform(transformer, false, 1, &x, &y, &z, &success) || !success) {
			continue;
		}

		//printf("%f, %f -> %f, %f\n", point.x, point.y, x, y);
		Point &p = points_out->addPoint(x, y);
		// TODO: copy local metadata
	}
	//exit(5);

	GDALDestroyReprojectionTransformer(transformer);

	return points_out_guard.release();
}







/**
 * Meteosat-projection
 */




const double  PI         =     3.14159265359;
const double  SAT_HEIGHT = 42164.0;       /* distance from Earth centre to satellite     */
const double  R_EQ       =  6378.169;     /* radius from Earth centre to equator         */
const double  R_POL      =  6356.5838;    /* radius from Earth centre to pol             */

const double  SUB_LON    =     0.0;       /* longitude of sub-satellite point in radiant */

const double  CFAC_NONHRV  =  -781648343;      /* scaling coefficients (see note above)  */
const double  LFAC_NONHRV  =  -781648343;      /* scaling coefficients (see note above)  */

const double  CFAC_HRV     =   -2344945030.;   /* scaling coefficients (see note above)  */
const double  LFAC_HRV     =   -2344945030.;   /* scaling coefficients (see note above)  */


const long    COFF_NONHRV  =        1856;      /* scaling coefficients (see note above)  */
const long    LOFF_NONHRV  =        1856;      /* scaling coefficients (see note above)  */

const long    COFF_HRV     =        5566;      /* scaling coefficients (see note above)  */
const long    LOFF_HRV     =        5566;      /* scaling coefficients (see note above)  */

static void pixcoord2geocoord(int column, int row, double *longitude, double *latitude) {
	const int coff = COFF_NONHRV;
	const int loff = LOFF_NONHRV;
	const double cfac = CFAC_NONHRV;
	const double lfac = LFAC_NONHRV;


  double s1=0.0, s2=0.0, s3=0.0, sn=0.0, sd=0.0, sxy=0.0, sa=0.0;
  double x=0.0, y=0.0;
  double longi=0.0, lati=0.0;

  int c=0, l=0;

  c=column;
  l=row;


  /*  calculate viewing angle of the satellite by use of the equation  */
  /*  on page 28, Ref [1]. */

  x = pow(2,16) * ( (double)c - (double)coff) / (double)cfac ;
  y = pow(2,16) * ( (double)l - (double)loff) / (double)lfac ;

  /*  now calculate the inverse projection */

  /* first check for visibility, whether the pixel is located on the Earth   */
  /* surface or in space. 						     */
  /* To do this calculate the argument to sqrt of "sd", which is named "sa". */
  /* If it is negative then the sqrt will return NaN and the pixel will be   */
  /* located in space, otherwise all is fine and the pixel is located on the */
  /* Earth surface.                                                          */

  sa =  pow(SAT_HEIGHT * cos(x) * cos(y),2 )  - (cos(y)*cos(y) + (double)1.006803 * sin(y)*sin(y)) * (double)1737121856. ;

  /* produce error values */
  if ( sa <= 0.0 ) {
    *latitude = -999.999;
    *longitude = -999.999;
    return;
  }


  /* now calculate the rest of the formulas using equations on */
  /* page 25, Ref. [1]  */

  sd = sqrt( pow((SAT_HEIGHT * cos(x) * cos(y)),2)
	     - (cos(y)*cos(y) + (double)1.006803 * sin(y)*sin(y)) * (double)1737121856. );
  sn = (SAT_HEIGHT * cos(x) * cos(y) - sd)
    / ( cos(y)*cos(y) + (double)1.006803 * sin(y)*sin(y) ) ;

  s1 = SAT_HEIGHT - sn * cos(x) * cos(y);
  s2 = sn * sin(x) * cos(y);
  s3 = -sn * sin(y);

  sxy = sqrt( s1*s1 + s2*s2 );

  /* using the previous calculations the inverse projection can be  */
  /* calculated now, which means calculating the lat./long. from    */
  /* the pixel row and column by equations on page 25, Ref [1].     */

  longi = atan(s2/s1) + SUB_LON;
  lati  = atan(((double)1.006803*s3)/sxy);
  /* convert from radians into degrees */
  *latitude = lati*180./PI;
  *longitude = longi*180./PI;
}

/*
int nint(double val) {
	return (int) round(val);
}
*/

static int nint(double val){

  double a=0.0; /* integral  part of val */
  double b=0.0; /* frational part of val */

  b = modf(val,&a);

  if ( b > 0.5 ){
    val = ceil(val);
  }
  else{
    val = floor(val);
  }

  return (int)val;

}

static void geocoord2pixcoord(double longitude, double latitude, int *column, int *row) {
	const int coff = COFF_NONHRV;
	const int loff = LOFF_NONHRV;
	const double cfac = CFAC_NONHRV;
	const double lfac = LFAC_NONHRV;

  int ccc=0, lll=0;

  double lati=0.0, longi=0.0;
  double c_lat=0.0;
  double lat=0.0;
  double lon=0.0;
  double r1=0.0, r2=0.0, r3=0.0, rn=0.0, re=0.0, rl=0.0;
  double xx=0.0, yy=0.0;
  double cc=0.0, ll=0.0;
  double dotprod=0.0;

  lati= latitude;
  longi= longitude;

  /* check if the values are sane, otherwise return error values */
  if (lati < -90.0 || lati > 90.0 || longi < -180.0 || longi > 180.0 ){
    *row = -1;
    *column = -1;
    return;
  }


  /* convert them to radiants */
  lat = lati*PI / (double)180.;
  lon = longi *PI / (double)180.;

  /* calculate the geocentric latitude from the          */
  /* geograhpic one using equations on page 24, Ref. [1] */

  c_lat = atan ( ((double)0.993243*(sin(lat)/cos(lat)) ));


  /* using c_lat calculate the length form the Earth */
  /* centre to the surface of the Earth ellipsoid    */
  /* equations on page 23, Ref. [1]                  */

  re = R_POL / sqrt( ((double)1.0 - (double)0.00675701 * cos(c_lat) * cos(c_lat) ) );


  /* calculate the forward projection using equations on */
  /* page 24, Ref. [1]                                        */

  rl = re;
  r1 = SAT_HEIGHT - rl * cos(c_lat) * cos(lon - SUB_LON);
  r2 = - rl *  cos(c_lat) * sin(lon - SUB_LON);
  r3 = rl * sin(c_lat);
  rn = sqrt( r1*r1 + r2*r2 +r3*r3 );


  /* check for visibility, whether the point on the Earth given by the */
  /* latitude/longitude pair is visible from the satellte or not. This */
  /* is given by the dot product between the vectors of:               */
  /* 1) the point to the spacecraft,			               */
  /* 2) the point to the centre of the Earth.			       */
  /* If the dot product is positive the point is visible otherwise it  */
  /* is invisible.						       */

  dotprod = r1*(rl * cos(c_lat) * cos(lon - SUB_LON)) - r2*r2 - r3*r3*(pow((R_EQ/R_POL),2));

  if (dotprod <= 0 ){
    *column = -1;
    *row = -1;
    return;
  }


  /* the forward projection is x and y */

  xx = atan( (-r2/r1) );
  yy = asin( (-r3/rn) );


  /* convert to pixel column and row using the scaling functions on */
  /* page 28, Ref. [1]. And finding nearest integer value for them. */


  cc = coff + xx *  pow(2,-16) * cfac ;
  ll = loff + yy *  pow(2,-16) * lfac ;


  ccc=nint(cc);
  lll=nint(ll);

  *column = ccc;
  *row = lll;
}








#if 0
template<typename T>
struct meteosat_draw_latlong{
	static GenericRaster *execute(Raster2D<T> *raster_src) {
		std::unique_ptr<GenericRaster> raster_src_guard(raster_src);

		if (raster_src->lcrs.epsg != EPSG_METEOSAT2)
			throw OperatorException("Source raster not in meteosat projection");

		raster_src->setRepresentation(GenericRaster::Representation::CPU);

		T max = raster_src->dd.max*2;
		DataDescription vm_dest(raster_src->dd.datatype, raster_src->dd.min, max, raster_src->dd.has_no_data, raster_src->dd.no_data);
		Raster2D<T> *raster_dest = (Raster2D<T> *) GenericRaster::create(raster_src->lcrs, vm_dest);
		std::unique_ptr<GenericRaster> raster_dest_guard(raster_dest);

		// erstmal alles kopieren
		int width = raster_dest->lcrs.size[0];
		int height = raster_dest->lcrs.size[1];
		for (int y=0;y<height;y++) {
			for (int x=0;x<width;x++) {
				raster_dest->set(x, y, raster_src->get(x, y));
			}
		}

		// LATLONG zeichnen
		for (float lon=-180;lon<=180;lon+=10) {
			for (float lat=-90;lat<=90;lat+=0.01) {
				int px, py;
				geocoord2pixcoord(lon, lat, &px, &py);
				if (px < 0 || py < 0)
					continue;
				raster_dest->set(px, py, max);
			}
		}


		for (float lon=-180;lon<=180;lon+=0.01) {
			for (float lat=-90;lat<=90;lat+=10) {
				int px, py;
				geocoord2pixcoord(lon, lat, &px, &py);
				if (px < 0 || py < 0)
					continue;
				raster_dest->set(px, py, max);
			}
		}

		return raster_dest_guard.release();
	}
};


MeteosatLatLongOperator::MeteosatLatLongOperator(int sourcecount, GenericOperator *sources[]) : GenericOperator(Type::RASTER, sourcecount, sources) {
	assumeSources(1);
}
MeteosatLatLongOperator::~MeteosatLatLongOperator() {
}

GenericRaster *MeteosatLatLongOperator::getRaster(const QueryRectangle &rect) {

	GenericRaster *raster = sources[0]->getRaster(rect);

	Profiler::Profiler p("METEOSAT_DRAW_LATLONG_OPERATOR");
	return callUnaryOperatorFunc<meteosat_draw_latlong>(raster);
}

#endif


