
#include "datatypes/raster/raster_priv.h"
#include "util/gdal.h"

#include <stdint.h>
#include <cstdlib>
#include <string>
#include <sstream>




static std::unique_ptr<GenericRaster> GDALImporter_loadRaster(GDALDataset *dataset, int rasteridx, double origin_x, double origin_y, double scale_x, double scale_y, epsg_t default_epsg) {
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
	// Read Pixel data
	int   nXSize = poBand->GetXSize();
	int   nYSize = poBand->GetYSize();

	epsg_t epsg = default_epsg; //EPSG_UNKNOWN;

	LocalCRS lcrs(epsg, nXSize, nYSize, origin_x, origin_y, scale_x, scale_y);
	double minvalue = adfMinMax[0];
	double maxvalue = adfMinMax[1];

	//if (type == GDT_Byte) maxvalue = 255;
	if(epsg == EPSG_GEOSMSG){
		hasnodata = true;
		nodata = 0;
		type = GDT_Int16; // TODO: sollte GDT_UInt16 sein!
	}


	DataDescription dd(type, minvalue, maxvalue, hasnodata, nodata);
	//printf("loading raster with %g -> %g valuerange\n", adfMinMax[0], adfMinMax[1]);

	auto raster = GenericRaster::create(lcrs, dd);
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

	poBand->RasterIO( GF_Read, 0, 0, nXSize, nYSize,
		buffer, nXSize, nYSize, type, 0, 0);

	// Selectively read metadata
	//char **mdList = GDALGetMetadata(poBand, "msg");
	if (epsg == EPSG_GEOSMSG) {
		char **mdList = poBand->GetMetadata("msg");
		for (int i = 0; mdList && mdList[i] != nullptr; i++ ) {
			printf("GDALImport: got Metadata %s\n", mdList[i]);
			std::string md(mdList[i]);
			size_t split = md.find('=');
			if (split == std::string::npos)
				continue;

			std::string key = md.substr(0, split);
			std::string value = md.substr(split+1, std::string::npos);

			if (key == "TimeStamp")
				raster->md_string.set(key, value);
			else if (key == "CalibrationOffset" || key == "CalibrationSlope") {
				double dvalue = std::strtod(value.c_str(), nullptr);
				raster->md_value.set(key, dvalue);
			}
		}
	}

	return raster;
}

std::unique_ptr<GenericRaster> GenericRaster::fromGDAL(const char *filename, int rasterid, epsg_t epsg) {
	GDAL::init();

	GDALDataset *dataset = (GDALDataset *) GDALOpen(filename, GA_ReadOnly);

	if (dataset == NULL)
		throw ImporterException("Could not open dataset");

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


	const char *drivername = dataset->GetDriverName();
	//printf("Driver: %s\n", drivername);
	if (strcmp(drivername, "MSG") == 0) {
		if (epsg != EPSG_GEOSMSG)
			throw ImporterException("MSG driver can only import rasters in MSG projection");
	}

	auto raster = GDALImporter_loadRaster(dataset, rasterid, adfGeoTransform[0], adfGeoTransform[3], adfGeoTransform[1], adfGeoTransform[5], epsg);

	GDALClose(dataset);

	return raster;
}

template<typename T> void Raster2D<T>::toGDAL(const char *filename, const char *gdalDriverName) {
	GDAL::init();

	GDALDriver *poDriver;
	GDALDataset *poDstDS;
    GDALRasterBand *poBand;
    char **papszMetadata;

	int count = GetGDALDriverManager()->GetDriverCount();
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
	poDstDS = poDriver->Create( filename, lcrs.size[0], lcrs.size[1], 1, dd.datatype, NULL);

	//set the affine transformation coefficients for pixel <-> world conversion and create the spatial reference and
	double adfGeoTransform[6]{ lcrs.origin[0], lcrs.scale[0], 0, lcrs.origin[1], 0, lcrs.scale[1] };
	std::string srs = GDAL::SRSFromEPSG(lcrs.epsg);
	//set dataset parameters
	poDstDS->SetGeoTransform(adfGeoTransform);
	poDstDS->SetProjection(srs.c_str());
	//get the dataset -> TODO: we only have one band at the moment
	void * data = const_cast<void *>(this->getData());
	poBand = poDstDS->GetRasterBand(1);

	if (dd.has_no_data){
			poBand->SetNoDataValue(dd.no_data);
	}

	poBand->RasterIO( GF_Write, 0, 0, lcrs.size[0], lcrs.size[1], data, lcrs.size[0], lcrs.size[1], dd.datatype, 0, 0 );


	//add the metadata to the dataset
	poDstDS->SetMetadataItem("test1","test2","UMR_MAPPING");
	//close all GDAL
	GDALClose( (GDALDatasetH) poDstDS );

}

RASTER_PRIV_INSTANTIATE_ALL
