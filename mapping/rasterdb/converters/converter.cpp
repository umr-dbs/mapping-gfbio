#include "datatypes/raster.h"
#include "datatypes/raster/raster_priv.h"
#include "rasterdb/converters/converter.h"

#include <memory>
#include <unordered_map>
#include "util/make_unique.h"

/**
 * Converter Registration
 */
// The magic of type registration, see REGISTER_RASTERCONVERTER in converter.h
typedef std::unique_ptr<RasterConverter> (*ConverterConstructor)();

static std::unordered_map< std::string, ConverterConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, ConverterConstructor > registered_constructors;
	return &registered_constructors;
}

RasterConverterRegistration::RasterConverterRegistration(const char *name, ConverterConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}


/**
 * static helper functions
 */
std::unique_ptr<ByteBuffer>RasterConverter::direct_encode(GenericRaster *raster, const std::string &method) {
	auto converter = getConverter(method);
	return converter->encode(raster);
}

std::unique_ptr<GenericRaster> RasterConverter::direct_decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, const std::string &method) {
	auto converter = getConverter(method);
	return converter->decode(buffer, datadescription, stref, width, height, depth);
}

std::unique_ptr<RasterConverter> RasterConverter::getConverter(const std::string &method) {
	auto map = getRegisteredConstructorsMap();
	if (map->count(method) != 1)
		throw ConverterException(concat("Unknown compression method ", method));
	auto constructor = map->at(method);
	return constructor();
}
