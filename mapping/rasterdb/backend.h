#ifndef RASTERDB_BACKEND_H
#define RASTERDB_BACKEND_H

#include "converters/converter.h"
#include "datatypes/attributes.h"
#include <stdint.h>
#include <vector>



class RasterDBBackend {
	public:
		using rasterid = int64_t;
		using tileid = int64_t;

		class TileDescription {
			public:
				tileid tileid;
				int channelid;
				int fileid;
				size_t offset;
				size_t size;
				uint32_t x1, y1, z1;
				uint32_t width, height, depth;
				RasterConverter::Compression compression;
		};

		virtual ~RasterDBBackend() {};

		virtual std::string readJSON() = 0;

		virtual rasterid createRaster(int channel, double time_start, double time_end, const DirectMetadata<std::string> &md_string, const DirectMetadata<double> &md_value) = 0;
		virtual void writeTile(rasterid rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression) = 0;

		virtual rasterid getClosestRaster(int channelid, double timestamp) = 0;
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