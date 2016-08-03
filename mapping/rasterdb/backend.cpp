
#include "rasterdb/backend.h"
#include "util/binarystream.h"

#include <unordered_map>


RasterDBBackend::TileDescription::TileDescription(BinaryReadBuffer &buffer) {
	buffer.read(&tileid);
	buffer.read(&channelid);
	buffer.read(&fileid);
	buffer.read(&offset);
	buffer.read(&size);
	buffer.read(&x1);
	buffer.read(&y1);
	buffer.read(&z1);
	buffer.read(&width);
	buffer.read(&height);
	buffer.read(&depth);
	buffer.read(&compression);
}

void RasterDBBackend::TileDescription::serialize(BinaryWriteBuffer &buffer, bool) const {
	buffer
		<< tileid
		<< channelid
		<< fileid
		<< offset
		<< size
		<< x1 << y1 << z1
		<< width << height
		<< depth
		<< compression
	;
}

RasterDBBackend::RasterDescription::RasterDescription(BinaryReadBuffer &buffer) {
	buffer.read(&rasterid);
	buffer.read(&time_start);
	buffer.read(&time_end);
}

void RasterDBBackend::RasterDescription::serialize(BinaryWriteBuffer &buffer, bool) const {
	buffer << rasterid;
	buffer << time_start << time_end;
}



RasterDBBackend::rasterid_t RasterDBBackend::createRaster(int channel, double time_start, double time_end, const AttributeMaps &attributes) {
	throw std::runtime_error("RasterDBBackend::createRaster() not implemented in this backend");
}

void RasterDBBackend::writeTile(rasterid_t rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, const std::string &compression) {
	throw std::runtime_error("RasterDBBackend::writeTile() not implemented in this backend");
}

void RasterDBBackend::linkRaster(int channelid, double time_of_reference, double time_start, double time_end) {
	throw std::runtime_error("RasterDBBackend::linkRaster() not implemented in this backend");
}



// RasterDB registration
typedef std::unique_ptr<RasterDBBackend> (*BackendConstructor)(const std::string &location);

static std::unordered_map< std::string, BackendConstructor > *getRegisteredConstructorsMap() {
	static std::unordered_map< std::string, BackendConstructor > registered_constructors;
	return &registered_constructors;
}

RasterDBBackendRegistration::RasterDBBackendRegistration(const char *name, BackendConstructor constructor) {
	auto map = getRegisteredConstructorsMap();
	(*map)[std::string(name)] = constructor;
}

std::unique_ptr<RasterDBBackend> RasterDBBackend::create(const std::string &backend, const std::string &location) {
	auto map = getRegisteredConstructorsMap();
	if (map->count(backend) != 1)
		throw ArgumentException(concat("Unknown rasterdb backend: ", backend));

	auto constructor = map->at(backend);
	return constructor(location);
}
