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
		getConverter(raster->rastermeta, raster->valuemeta, method)
	);

	return converter->encode(raster);
}

GenericRaster *RasterConverter::direct_decode(const RasterMetadata &rastermetadata, const ValueMetadata &valuemetadata, ByteBuffer *buffer, GenericRaster::Compression method) {
	std::unique_ptr<RasterConverter> converter(
		getConverter(rastermetadata, valuemetadata, method)
	);

	return converter->decode(buffer);
}

RasterConverter *RasterConverter::getConverter(const RasterMetadata &rm, const ValueMetadata &vm, GenericRaster::Compression method) {
	switch (method) {
		case GenericRaster::Compression::UNCOMPRESSED:
			return new RawConverter(rm, vm);
		case GenericRaster::Compression::GZIP:
			return new GzipConverter(rm, vm);
		case GenericRaster::Compression::BZIP:
			return new BzipConverter(rm, vm);
		default:
			throw ConverterException("Unsupported converter type");
	}
}
