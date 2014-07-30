#include "raster/raster.h"
#include "raster/raster_priv.h"
#include "converters/converter.h"
#include "converters/raw.h"

#include <memory>
#include "util/make_unique.h"

/**
 * static helper functions
 */
std::unique_ptr<ByteBuffer>RasterConverter::direct_encode(GenericRaster *raster, GenericRaster::Compression method) {
	auto converter = getConverter(raster->lcrs, raster->dd, method);
	return converter->encode(raster);
}

std::unique_ptr<GenericRaster> RasterConverter::direct_decode(const LocalCRS &localcrs, const DataDescription &datadescriptor, ByteBuffer *buffer, GenericRaster::Compression method) {
	auto converter = getConverter(localcrs, datadescriptor, method);
	return converter->decode(buffer);
}

std::unique_ptr<RasterConverter> RasterConverter::getConverter(const LocalCRS &localcrs, const DataDescription &datadescriptor, GenericRaster::Compression method) {
	switch (method) {
		case GenericRaster::Compression::UNCOMPRESSED:
			return std::make_unique<RawConverter>(localcrs, datadescriptor);
		case GenericRaster::Compression::GZIP:
			return std::make_unique<GzipConverter>(localcrs, datadescriptor);
		case GenericRaster::Compression::BZIP:
			return std::make_unique<BzipConverter>(localcrs, datadescriptor);
		default:
			throw ConverterException("Unsupported converter type");
	}
}
