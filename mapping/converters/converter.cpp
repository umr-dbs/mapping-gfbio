#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "converters/converter.h"
#include "converters/raw.h"

#include <memory>
#include "util/make_unique.h"

/**
 * static helper functions
 */
std::unique_ptr<ByteBuffer>RasterConverter::direct_encode(GenericRaster *raster, GenericRaster::Compression method) {
	auto converter = getConverter(method);
	return converter->encode(raster);
}

std::unique_ptr<GenericRaster> RasterConverter::direct_decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, GenericRaster::Compression method) {
	auto converter = getConverter(method);
	return converter->decode(buffer, datadescription, stref, width, height, depth);
}

std::unique_ptr<RasterConverter> RasterConverter::getConverter(GenericRaster::Compression method) {
	switch (method) {
		case GenericRaster::Compression::UNCOMPRESSED:
			return std::make_unique<RawConverter>();
		case GenericRaster::Compression::GZIP:
			return std::make_unique<GzipConverter>();
		case GenericRaster::Compression::BZIP:
			return std::make_unique<BzipConverter>();
		default:
			throw ConverterException("Unsupported converter type");
	}
}
