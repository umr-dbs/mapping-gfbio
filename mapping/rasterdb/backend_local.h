#ifndef RASTERDB_BACKEND_LOCAL_H
#define RASTERDB_BACKEND_LOCAL_H

//#include "rasterdb/rasterdb.h"
#include "rasterdb/backend.h"
#include "util/sqlite.h"

#include <string>


class LocalRasterDBBackend : public RasterDBBackend {
	public:
		LocalRasterDBBackend(const char *filename, bool writeable = false);
		virtual ~LocalRasterDBBackend();


		virtual std::string readJSON();
		virtual rasterid_t createRaster(int channel, double time_start, double time_end, const DirectMetadata<std::string> &md_string, const DirectMetadata<double> &md_value);
		virtual void writeTile(rasterid_t rasterid, ByteBuffer &buffer, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, RasterConverter::Compression compression);
		virtual void linkRaster(int channelid, double time_of_reference, double time_start, double time_end);


		virtual RasterDescription getClosestRaster(int channelid, double t1, double t2);
		virtual void readAttributes(rasterid_t rasterid, DirectMetadata<std::string> &md_string, DirectMetadata<double> &md_value);
		virtual int getBestZoom(rasterid_t rasterid, int desiredzoom);
		virtual const std::vector<TileDescription> enumerateTiles(int channelid, rasterid_t rasterid, int x1, int y1, int x2, int y2, int zoom = 0);
		virtual bool hasTile(rasterid_t rasterid, uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom);
		virtual std::unique_ptr<ByteBuffer> readTile(const TileDescription &tiledesc);

	private:
		void init();
		void cleanup();

		int lockedfile;
		std::string sourcename;
		std::string filename_json;
		std::string filename_data;
		std::string filename_db;
		std::string json;
		SQLite db;
};

#endif
