#ifndef RASTER_RASTERSOURCE_H
#define RASTER_RASTERSOURCE_H

#include <stdint.h>
#include <exception>
#include <unordered_map>
#include <mutex>

#include "raster/raster.h"
#include "util/sqlite.h"

class RasterSourceChannel;

class RasterSource {
	public:
		static const bool READ_ONLY = false;
		static const bool READ_WRITE = true;
	private: // Instantiation only by RasterSourceManager
		RasterSource(const char *filename, bool writeable = RasterSource::READ_ONLY);
		virtual ~RasterSource();
		friend class RasterSourceManager;

	public:
		void import(const char *filename, int sourcechannel, int channelid, time_t timestamp, GenericRaster::Compression compression = GenericRaster::Compression::GZIP);
		void import(GenericRaster *raster, int channelid, time_t timestamp, GenericRaster::Compression compression = GenericRaster::Compression::GZIP);

		std::unique_ptr<GenericRaster> load(int channelid, time_t timestamp, int x1, int y1, int x2, int y2, int zoom = 0, bool transform = true);

		const LocalCRS *getLocalCRS() const { return lcrs; };

		bool isWriteable() const { return writeable; }

	private:
		bool hasTile(const LocalCRS &lcrs, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp);
		void importTile(GenericRaster *raster, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp, GenericRaster::Compression compression = GenericRaster::Compression::PREDICTED);
		std::unique_ptr<GenericRaster> loadTile(int channelid, const LocalCRS &tilecrs, int fileid, size_t offset, size_t size, GenericRaster::Compression method);

		void init();
		void cleanup();

		int lockedfile;
		bool writeable;
		std::string filename_json;
		std::string filename_data;
		std::string filename_db;
		LocalCRS *lcrs;
		int channelcount;
		RasterSourceChannel **channels;
		SQLite db;
		int refcount;
};


/*
 * Each RasterSource has a lock on its files, so no two open objects should refer to the same source.
 * Constructing and Destructing RasterSources through the manager solves this.
 */
class RasterSourceManager {
	public:
		static RasterSource *open(const char *filename, bool writeable = RasterSource::READ_ONLY);
		static void close(RasterSource *source);

	private:
		static std::unordered_map<std::string, RasterSource *> map;
		static std::mutex mutex;
		RasterSourceManager();
};

#endif
