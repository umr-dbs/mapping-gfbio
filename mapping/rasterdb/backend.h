#ifndef RASTERDB_BACKEND_H
#define RASTERDB_BACKEND_H

#include "converters/converter.h"
#include "datatypes/attributes.h"
#include <stdint.h>
#include <vector>

class BinaryStream;

class RasterDBBackend {
	public:
		using rasterid = int64_t;
		using tileid = int64_t;

		class TileDescription {
			public:
				TileDescription(tileid tileid, int channelid, int fileid, size_t offset, size_t size, uint32_t x1, uint32_t y1, uint32_t z1, uint32_t width, uint32_t height, uint32_t depth, RasterConverter::Compression compression)
					: tileid(tileid), channelid(channelid), fileid(fileid), offset(offset), size(size), x1(x1), y1(y1), z1(z1), width(width), height(height), depth(depth), compression(compression) {}
				TileDescription(BinaryStream &stream);
				void toStream(BinaryStream &stream) const;

				tileid tileid;
				int channelid;
				int fileid;
				size_t offset;
				size_t size;
				uint32_t x1, y1, z1;
				uint32_t width, height, depth;
				RasterConverter::Compression compression;
		};

		class RasterDescription {
			public:
				RasterDescription(rasterid rasterid, double time_start, double time_end) : rasterid(rasterid), time_start(time_start), time_end(time_end) {}
				RasterDescription(BinaryStream &stream);
				void toStream(BinaryStream &stream) const;

				rasterid rasterid;
				double time_start;
				double time_end;
		};

		virtual ~RasterDBBackend() {};

		virtual std::string readJSON() = 0;

		virtual rasterid createRaster(int channel, double time_start, double time_end, const DirectMetadata<std::string> &md_string, const DirectMetadata<double> &md_value);
		virtual void writeTile(rasterid rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression);
		virtual void linkRaster(int channelid, double time_of_reference, double time_start, double time_end);

		virtual RasterDescription getClosestRaster(int channelid, double timestamp) = 0;
		virtual void readAttributes(rasterid rasterid, DirectMetadata<std::string> &md_string, DirectMetadata<double> &md_value) = 0;
		virtual int getBestZoom(rasterid rasterid, int desiredzoom) = 0;
		virtual const std::vector<TileDescription> enumerateTiles(int channelid, rasterid rasterid, int x1, int y1, int x2, int y2, int zoom = 0) = 0;
		virtual bool hasTile(rasterid rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom) = 0;
		virtual std::unique_ptr<ByteBuffer> readTile(const TileDescription &tiledesc) = 0;

	protected:
		RasterDBBackend(bool writeable) : writeable(writeable) {};

		const bool writeable;
};

#endif
