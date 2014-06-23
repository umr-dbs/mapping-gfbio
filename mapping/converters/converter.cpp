#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "converters/converter.h"
#include "converters/raw.h"

#include <memory>


/**
 * static helper functions
 */
ByteBuffer *RasterConverter::direct_encode(GenericRaster *raster, GenericRaster::Compression method) {
	std::unique_ptr<RasterConverter> converter(
		getConverter(raster->lcrs, raster->dd, method)
	);

	return converter->encode(raster);
}

GenericRaster *RasterConverter::direct_decode(const LocalCRS &localcrs, const DataDescription &datadescriptor, ByteBuffer *buffer, GenericRaster::Compression method) {
	std::unique_ptr<RasterConverter> converter(
		getConverter(localcrs, datadescriptor, method)
	);

	return converter->decode(buffer);
}

RasterConverter *RasterConverter::getConverter(const LocalCRS &localcrs, const DataDescription &datadescriptor, GenericRaster::Compression method) {
	switch (method) {
		case GenericRaster::Compression::UNCOMPRESSED:
			return new RawConverter(localcrs, datadescriptor);
		case GenericRaster::Compression::GZIP:
			return new GzipConverter(localcrs, datadescriptor);
		case GenericRaster::Compression::BZIP:
			return new BzipConverter(localcrs, datadescriptor);
		default:
			throw ConverterException("Unsupported converter type");
	}
}
