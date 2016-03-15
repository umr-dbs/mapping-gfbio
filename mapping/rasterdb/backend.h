#ifndef RASTERDB_BACKEND_H
#define RASTERDB_BACKEND_H

#include "converters/converter.h"
#include "datatypes/attributes.h"
#include <stdint.h>
#include <vector>

class BinaryReadBuffer;
class BinaryWriteBuffer;

class RasterDBBackend {
	public:
		using rasterid_t = int64_t;
		using tileid_t = int64_t;

		class TileDescription {
			public:
				TileDescription(tileid_t tileid, int channelid, int fileid, size_t offset, size_t size, uint32_t x1, uint32_t y1, uint32_t z1, uint32_t width, uint32_t height, uint32_t depth, RasterConverter::Compression compression)
					: tileid(tileid), channelid(channelid), fileid(fileid), offset(offset), size(size), x1(x1), y1(y1), z1(z1), width(width), height(height), depth(depth), compression(compression) {}
				TileDescription(BinaryReadBuffer &buffer);
				void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

				tileid_t tileid;
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
				RasterDescription(rasterid_t rasterid, double time_start, double time_end) : rasterid(rasterid), time_start(time_start), time_end(time_end) {}
				RasterDescription(BinaryReadBuffer &buffer);
				void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

				rasterid_t rasterid;
				double time_start;
				double time_end;
		};

		virtual ~RasterDBBackend() {};

		virtual std::vector<std::string> enumerateSources() = 0;
		virtual std::string readJSON(const std::string &sourcename) = 0;

		virtual void open(const std::string &sourcename, bool writeable) = 0;

		virtual rasterid_t createRaster(int channel, double time_start, double time_end, const AttributeMaps &global_attributes);
		virtual void writeTile(rasterid_t rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression);
		virtual void linkRaster(int channelid, double time_of_reference, double time_start, double time_end);

		virtual std::string readJSON() = 0;
		virtual RasterDescription getClosestRaster(int channelid, double t1, double t2) = 0;
		virtual void readAttributes(rasterid_t rasterid, AttributeMaps &global_attributes) = 0;
		virtual int getBestZoom(rasterid_t rasterid, int desiredzoom) = 0;
		virtual const std::vector<TileDescription> enumerateTiles(int channelid, rasterid_t rasterid, int x1, int y1, int x2, int y2, int zoom = 0) = 0;
		virtual bool hasTile(rasterid_t rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom) = 0;
		virtual std::unique_ptr<ByteBuffer> readTile(const TileDescription &tiledesc) = 0;

		bool isOpen() { return is_opened; }
		bool isWriteable() { return is_writeable; }
	protected:
		RasterDBBackend() : is_opened(false), is_writeable(false) {};

		bool is_opened;
		bool is_writeable;
};

#endif
