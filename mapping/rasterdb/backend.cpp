
#include "rasterdb/backend.h"
#include "util/binarystream.h"


RasterDBBackend::TileDescription::TileDescription(BinaryStream &stream) {
	stream.read(&tileid);
	stream.read(&channelid);
	stream.read(&fileid);
	stream.read(&offset);
	stream.read(&size);
	stream.read(&x1);
	stream.read(&y1);
	stream.read(&z1);
	stream.read(&width);
	stream.read(&height);
	stream.read(&depth);
	stream.read(&compression);
}

void RasterDBBackend::TileDescription::toStream(BinaryStream &stream) const {
	stream.write(tileid);
	stream.write(channelid);
	stream.write(fileid);
	stream.write(offset);
	stream.write(size);
	stream.write(x1);
	stream.write(y1);
	stream.write(z1);
	stream.write(width);
	stream.write(height);
	stream.write(depth);
	stream.write(compression);
}

RasterDBBackend::RasterDescription::RasterDescription(BinaryStream &stream) {
	stream.read(&rasterid);
	stream.read(&time_start);
	stream.read(&time_end);
}

void RasterDBBackend::RasterDescription::toStream(BinaryStream &stream) const {
	stream.write(rasterid);
	stream.write(time_start);
	stream.write(time_end);
}



RasterDBBackend::rasterid_t RasterDBBackend::createRaster(int channel, double time_start, double time_end, const AttributeMaps &attributes) {
	throw std::runtime_error("RasterDBBackend::createRaster() not implemented in this backend");
}

void RasterDBBackend::writeTile(rasterid_t rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression) {
	throw std::runtime_error("RasterDBBackend::writeTile() not implemented in this backend");
}

void RasterDBBackend::linkRaster(int channelid, double time_of_reference, double time_start, double time_end) {
	throw std::runtime_error("RasterDBBackend::linkRaster() not implemented in this backend");
}
