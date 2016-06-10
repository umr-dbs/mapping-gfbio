#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "rasterdb/converters/converter.h"
#include "rasterdb/converters/raw.h"

#include <memory>
#include <sstream>
#include "util/make_unique.h"

/**
 * static helper functions
 */
std::unique_ptr<ByteBuffer>RasterConverter::direct_encode(GenericRaster *raster, RasterConverter::Compression method) {
	auto converter = getConverter(method);
	return converter->encode(raster);
}

std::unique_ptr<GenericRaster> RasterConverter::direct_decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, RasterConverter::Compression method) {
	auto converter = getConverter(method);
	return converter->decode(buffer, datadescription, stref, width, height, depth);
}

std::unique_ptr<RasterConverter> RasterConverter::getConverter(RasterConverter::Compression method) {
	switch (method) {
		case RasterConverter::Compression::UNCOMPRESSED:
			return make_unique<RawConverter>();
		case RasterConverter::Compression::GZIP:
			return make_unique<GzipConverter>();
		case RasterConverter::Compression::BZIP:
			return make_unique<BzipConverter>();
		default:
			std::ostringstream msg;
			msg << "Unsupported converter type: " << method;
			throw ConverterException(msg.str());
	}
}
