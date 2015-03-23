
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
