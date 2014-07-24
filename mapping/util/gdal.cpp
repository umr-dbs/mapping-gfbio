

#include "raster/raster.h"
#include "util/gdal.h"

#include <stdint.h>
#include <cstdlib>
#include <mutex>
#include <sstream>
//#include <cmath>

#include <gdal_priv.h>
#include <gdal_alg.h>

#include <cpl_string.h>
#include <ogr_spatialref.h>


namespace GDAL {


static std::mutex gdal_init_mutex;
static bool gdal_is_initialized = false;

void init() {
	if (gdal_is_initialized)
		return;
	std::lock_guard<std::mutex> guard(gdal_init_mutex);
	if (gdal_is_initialized)
		return;
	gdal_is_initialized = true;
	GDALAllRegister();
	//GetGDALDriverManager()->AutoLoadDrivers();
}


std::string SRSFromEPSG(epsg_t epsg) {
		std::ostringstream epsg_name;
		epsg_name << "EPSG:" << epsg;

		std::string epsg_name_str = epsg_name.str();
		OGRSpatialReferenceH hSRS;
		char *pszResult = NULL;

		CPLErrorReset();

		hSRS = OSRNewSpatialReference( NULL );
		if( OSRSetFromUserInput( hSRS, epsg_name_str.c_str() ) == OGRERR_NONE )
				OSRExportToWkt( hSRS, &pszResult );
		else {
			std::ostringstream msg;
			msg << "SRS could not be parsed for epsg " << epsg;
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

	// Transforming from MSAT2: we transform to LATLON manually, then let GDAL do the rest.
	epsg_t transformer_in_epsg = in_epsg;
	if (in_epsg == EPSG_METEOSAT2)
		transformer_in_epsg = EPSG_LATLON;
	epsg_t transformer_out_epsg = out_epsg;
	if (out_epsg == EPSG_METEOSAT2)
		transformer_out_epsg = EPSG_LATLON;

	transformer = GDALCreateReprojectionTransformer(SRSFromEPSG(transformer_in_epsg).c_str(), SRSFromEPSG(transformer_out_epsg).c_str());
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
	if (in_epsg == EPSG_METEOSAT2) {
		// coords from MSAT to latlon
		msg_pixcoord2geocoord(3712-(int) px, (int) py, &px, &py);
		if (px < -180.0 || py < -90.0)
			return false;
	}
	if (in_epsg != out_epsg) {
		if (!GDALReprojectionTransform(transformer, false, 1, &px, &py, &pz, &success) || !success)
			return false;
	}
	if (out_epsg == EPSG_METEOSAT2) {
		// coords from latlon to MSAT
		int tx, ty;
		msg_geocoord2pixcoord(px, py, &tx, &ty);
		if (tx < 0 || ty < 0)
			return false;
		px = (double) (3712-tx);
		py = (double) ty;
	}

	return true;
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

void CRSTransformer::msg_pixcoord2geocoord(int column, int row, double *longitude, double *latitude) {
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

void CRSTransformer::msg_geocoord2pixcoord(double longitude, double latitude, int *column, int *row) {
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




} // End namespace GDAL
