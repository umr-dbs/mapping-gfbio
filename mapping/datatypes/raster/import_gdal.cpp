
#include "datatypes/raster/raster_priv.h"
#include "util/gdal.h"
#include "util/debug.h"

#include <stdint.h>
#include <cstdlib>
#include <string>
#include <sstream>




static std::unique_ptr<GenericRaster> GDALImporter_loadRaster(GDALDataset *dataset, int rasteridx, double origin_x, double origin_y, double scale_x, double scale_y, bool &flipx, bool &flipy, epsg_t default_epsg, bool clip, double clip_x1, double clip_y1, double clip_x2, double clip_y2) {
	GDALRasterBand  *poBand;
	int             nBlockXSize, nBlockYSize;
	int             bGotMin, bGotMax;
	double          adfMinMax[2];

	poBand = dataset->GetRasterBand( rasteridx );
	poBand->GetBlockSize( &nBlockXSize, &nBlockYSize );

	GDALDataType type = poBand->GetRasterDataType();

/*
	printf( "Block=%dx%d Type=%s, ColorInterp=%s\n",
		nBlockXSize, nBlockYSize,
		GDALGetDataTypeName(poBand->GetRasterDataType()),
		GDALGetColorInterpretationName(
				poBand->GetColorInterpretation()) );
*/
	adfMinMax[0] = poBand->GetMinimum( &bGotMin );
	adfMinMax[1] = poBand->GetMaximum( &bGotMax );
	if( ! (bGotMin && bGotMax) )
		GDALComputeRasterMinMax((GDALRasterBandH)poBand, TRUE, adfMinMax);

	int hasnodata = true;
	int success;
	double nodata = poBand->GetNoDataValue(&success);
	if (!success /*|| nodata < adfMinMax[0] || nodata > adfMinMax[1]*/) {
		hasnodata = false;
		nodata = 0;
	}
	/*
	if (nodata < adfMinMax[0] || nodata > adfMinMax[1]) {

	}
	*/
/*
	printf( "Min=%.3g, Max=%.3g\n", adfMinMax[0], adfMinMax[1] );

	if( poBand->GetOverviewCount() > 0 )
		printf( "Band has %d overviews.\n", poBand->GetOverviewCount() );

	if( poBand->GetColorTable() != NULL )
		printf( "Band has a color table with %d entries.\n",
			poBand->GetColorTable()->GetColorEntryCount() );
*/

	// Figure out the data type
	epsg_t epsg = default_epsg;
	double minvalue = adfMinMax[0];
	double maxvalue = adfMinMax[1];

	//if (type == GDT_Byte) maxvalue = 255;
	if(epsg == EPSG_GEOSMSG){
		hasnodata = true;
		nodata = 0;
		type = GDT_Int16; // TODO: sollte GDT_UInt16 sein!
	}

	// Figure out which pixels to load
	int   nXSize = poBand->GetXSize();
	int   nYSize = poBand->GetYSize();

	int pixel_x1 = 0;
	int pixel_y1 = 0;
	int pixel_width = nXSize;
	int pixel_height = nYSize;
	if (clip) {
		pixel_x1 = floor((clip_x1 - origin_x) / scale_x);
		pixel_y1 = floor((clip_y1 - origin_y) / scale_y);
		int pixel_x2 = floor((clip_x2 - origin_x) / scale_x);
		int pixel_y2 = floor((clip_y2 - origin_y) / scale_y);

		if (pixel_x1 > pixel_x2)
			std::swap(pixel_x1, pixel_x2);
		if (pixel_y1 > pixel_y2)
			std::swap(pixel_y1, pixel_y2);

		pixel_x1 = std::max(0, pixel_x1);
		pixel_y1 = std::max(0, pixel_y1);

		pixel_x2 = std::min(nXSize-1, pixel_x2);
		pixel_y2 = std::min(nYSize-1, pixel_y2);

		pixel_width = pixel_x2 - pixel_x1 + 1;
		pixel_height = pixel_y2 - pixel_y1 + 1;
	}

	{
		std::ostringstream msg;
		msg << "GDAL: loading " << pixel_x1 << "," << pixel_y1 << " -> +" << pixel_width << ",+" << pixel_height;
		d(msg.str());
	}

	double x1 = origin_x + scale_x * (pixel_x1 - 0.5);
	double y1 = origin_y + scale_y * (pixel_y1 - 0.5);
	double x2 = x1 + scale_x * pixel_width;
	double y2 = y1 + scale_y * pixel_height;

	SpatioTemporalReference stref(
		SpatialReference(epsg, x1, y1, x2, y2, flipx, flipy),
		TemporalReference::unreferenced()
	);
	Unit unit = Unit::unknown();
	unit.setMinMax(minvalue, maxvalue);
	DataDescription dd(type, unit, hasnodata, nodata);
	//printf("loading raster with %g -> %g valuerange\n", adfMinMax[0], adfMinMax[1]);

	auto raster = GenericRaster::create(dd, stref, pixel_width, pixel_height);
	void *buffer = raster->getDataForWriting();
	//int bpp = raster->getBPP();

	/*
CPLErr GDALRasterBand::RasterIO( GDALRWFlag eRWFlag,
                                 int nXOff, int nYOff, int nXSize, int nYSize,
                                 void * pData, int nBufXSize, int nBufYSize,
                                 GDALDataType eBufType,
                                 int nPixelSpace,
                                 int nLineSpace )
	*/

	// Read Pixel data
	auto res = poBand->RasterIO( GF_Read,
		pixel_x1, pixel_y1, pixel_width, pixel_height, // rectangle in the source raster
		buffer, raster->width, raster->height, // position and size of the destination buffer
		type, 0, 0);

	if (res != CE_None)
		throw ImporterException("GDAL: RasterIO failed");

	// Selectively read metadata
	//char **mdList = GDALGetMetadata(poBand, "msg");
	if (epsg == EPSG_GEOSMSG) {
		char **mdList = poBand->GetMetadata("msg");
		for (int i = 0; mdList && mdList[i] != nullptr; i++ ) {
			//printf("GDALImport: got Metadata %s\n", mdList[i]);
			std::string md(mdList[i]);
			size_t split = md.find('=');
			if (split == std::string::npos)
				continue;

			std::string key = md.substr(0, split);
			std::string mkey = "msg." + key;
			std::string value = md.substr(split+1, std::string::npos);

			double dvalue = std::strtod(value.c_str(), nullptr);
			if (key == "TimeStamp" || (dvalue == 0 && value != "0")) {
				raster->md_string.set(mkey, value);
			}
			else {
				raster->md_value.set(mkey, dvalue);
			}
		}
	}

	return raster;
}

static std::unique_ptr<GenericRaster> GDALImporter_loadDataset(const char *filename, int rasterid, bool &flipx, bool &flipy, epsg_t epsg, bool clip, double x1, double y1, double x2, double y2) {
	GDAL::init();

	GDALDataset *dataset = (GDALDataset *) GDALOpen(filename, GA_ReadOnly);

	if (dataset == NULL)
		throw ImporterException(concat("Could not open dataset ", filename));


/*
	printf( "Driver: %s/%s\n",
		dataset->GetDriver()->GetDescription(),
		dataset->GetDriver()->GetMetadataItem( GDAL_DMD_LONGNAME ) );

	printf( "Size is %dx%dx%d\n",
		dataset->GetRasterXSize(), dataset->GetRasterYSize(),
		dataset->GetRasterCount() );

	if( dataset->GetProjectionRef()  != NULL )
			printf( "Projection is `%s'\n", dataset->GetProjectionRef() );
*/
	double adfGeoTransform[6];

	// http://www.gdal.org/classGDALDataset.html#af9593cc241e7d140f5f3c4798a43a668
	if( dataset->GetGeoTransform( adfGeoTransform ) != CE_None ) {
		GDALClose(dataset);
		throw ImporterException("no GeoTransform information in raster");
	}
/*
	if (adfGeoTransform[2] != 0 || adfGeoTransform[4] != 0) {
		GDALClose(dataset);
		std::stringstream ss;
		ss << "Raster is using an unsupported GeoTransform: (" << adfGeoTransform[0] << "/" << adfGeoTransform[1] << "/" << adfGeoTransform[2] << "/" << adfGeoTransform[3] << ")";
		throw ImporterException(ss.str());
	}
*/
/*
	{
		printf( "Origin = (%.6f,%.6f)\n",
			adfGeoTransform[0], adfGeoTransform[3] );

		printf( "Pixel Size = (%.6f,%.6f)\n",
			adfGeoTransform[1], adfGeoTransform[5] );
	}
*/

	int rastercount = dataset->GetRasterCount();

	if (rasterid < 1 || rasterid > rastercount) {
		GDALClose(dataset);
		throw ImporterException("rasterid not found");
	}


	const char *drivername = dataset->GetDriver()->GetDescription();
	//const char *drivername = dataset->GetDriverName();
	//printf("Driver: %s\n", drivername);
	if (strcmp(drivername, "MSG") == 0) {
		if (epsg != EPSG_GEOSMSG)
			throw ImporterException("MSG driver can only import rasters in MSG projection");
	}

	auto raster = GDALImporter_loadRaster(dataset, rasterid, adfGeoTransform[0], adfGeoTransform[3], adfGeoTransform[1], adfGeoTransform[5], flipx, flipy, epsg, clip, x1, y1, x2, y2);

	GDALClose(dataset);

	return raster;
}


std::unique_ptr<GenericRaster> GenericRaster::fromGDAL(const char *filename, int rasterid, bool &flipx, bool &flipy, epsg_t epsg, double x1, double y1, double x2, double y2) {
	return GDALImporter_loadDataset(filename, rasterid, flipx, flipy, epsg, true, x1, y1, x2, y2);
}

std::unique_ptr<GenericRaster> GenericRaster::fromGDAL(const char *filename, int rasterid, bool &flipx, bool &flipy, epsg_t epsg) {
	return GDALImporter_loadDataset(filename, rasterid, flipx, flipy, epsg, false, 0, 0, 0, 0);
}


std::unique_ptr<GenericRaster> GenericRaster::fromGDAL(const char *filename, int rasterid, epsg_t epsg) {
	bool flipx, flipy;
	auto result = fromGDAL(filename, rasterid, flipx, flipy, epsg);

	if (flipx || flipy) {
		result = result->flip(flipx, flipy);
	}
	return result;
}




template<typename T> void Raster2D<T>::toGDAL(const char *filename, const char *gdalDriverName, bool flipx, bool flipy) {
	GDAL::init();

	GDALDriver *poDriver;
	GDALDataset *poDstDS;
    GDALRasterBand *poBand;
    //char **papszMetadata;

//	int count = GetGDALDriverManager()->GetDriverCount();
//	printf("GDAL has %d drivers\n", count);
//	for (int i=0;i<count;i++) {
//		poDriver = GetGDALDriverManager()->GetDriver(i);
//
//		if( poDriver == NULL ) {
//			continue;
//		}
//
//		papszMetadata = poDriver->GetMetadata();
//		if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATE, FALSE ) )
//			printf( "Driver %d supports Create() method.\n", i );
//		if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATECOPY, FALSE ) )
//			printf( "Driver %d supports CreateCopy() method.\n", i );
//
//	}

	poDriver = GetGDALDriverManager()->GetDriverByName(gdalDriverName);

//	if( poDriver == NULL ) {
//		printf( "No Driver found for FormatName %s.\n", gdalFormatName);
//		return;
//	}
//
//	papszMetadata = poDriver->GetMetadata();
//	if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATE, FALSE ) )
//		printf( "Driver %s supports Create() method.\n", gdalFormatName);
//	if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATECOPY, FALSE ) )
//		printf( "Driver %s supports CreateCopy() method.\n", gdalFormatName);

	//now create a GDAL dataset using the driver for gdalFormatName
	char **papszOptions = nullptr;

	if (strcmp(gdalDriverName, "GTiff") == 0) {
		papszOptions = CSLSetNameValue(papszOptions, "COMPRESS", "DEFLATE");
	}

	poDstDS = poDriver->Create( filename, width, height, 1, dd.datatype, papszOptions);

	CSLDestroy(papszOptions);

	//set the affine transformation coefficients for pixel <-> world conversion and create the spatial reference and
	double scale_x = pixel_scale_x * (flipx ? -1 : 1);
	double scale_y = pixel_scale_y * (flipy ? -1 : 1);
	double origin_x = flipx ? stref.x2 : stref.x1;
	double origin_y = flipy ? stref.y2 : stref.y1;

	double adfGeoTransform[6]{ origin_x, scale_x, 0, origin_y, 0, scale_y };
	std::string srs = GDAL::SRSFromEPSG(stref.epsg);
	//set dataset parameters
	poDstDS->SetGeoTransform(adfGeoTransform);
	poDstDS->SetProjection(srs.c_str());
	//get the dataset -> TODO: we only have one band at the moment
	void * data = const_cast<void *>(this->getData());
	poBand = poDstDS->GetRasterBand(1);

	if (dd.has_no_data){
			poBand->SetNoDataValue(dd.no_data);
	}

	auto res = poBand->RasterIO( GF_Write, 0, 0, width, height, data, width, height, dd.datatype, 0, 0 );
	if (res != CE_None)
		throw ImporterException("GDAL: RasterIO for writing failed");

	//add the metadata to the dataset
	//poDstDS->SetMetadataItem("test1","test2","UMR_MAPPING");

	//close all GDAL
	GDALClose( (GDALDatasetH) poDstDS );

}

RASTER_PRIV_INSTANTIATE_ALL
