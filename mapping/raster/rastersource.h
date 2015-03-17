#ifndef RASTER_RASTERSOURCE_H
#define RASTER_RASTERSOURCE_H

#include <stdint.h>
#include <exception>
#include <unordered_map>
#include <mutex>

#include "datatypes/raster.h"
#include "util/sqlite.h"

class RasterSourceChannel;
class QueryRectangle;
class QueryProfiler;


/*
 * A coordinate reference system as modeled by GDAL.
 * See http://www.gdal.org/gdal_datamodel.html for the formulas.
 * Take note that the origin is not the center of pixel (0,0), but the outer corner!
 * This is different from the CRS used e.g. in OpenCL kernels, where origin is set to the center of pixel (0,0).
 *
 */
class GDALCRS {
	public:
		GDALCRS(epsg_t epsg, uint32_t w, uint32_t h, double origin_x, double origin_y, double scale_x, double scale_y)
			: epsg(epsg), dimensions(2), size{w, h, 0}, origin{origin_x, origin_y, 0}, scale{scale_x, scale_y, 0} {};

		GDALCRS(epsg_t epsg, int dimensions, uint32_t w, uint32_t h, uint32_t d, double origin_x, double origin_y, double origin_z, double scale_x, double scale_y, double scale_z)
			: epsg(epsg), dimensions(dimensions), size{w, h, d}, origin{origin_x, origin_y, origin_z}, scale{scale_x, scale_y, scale_z} {};

		GDALCRS(const GridSpatioTemporalResult &stres)
			: epsg(stres.stref.epsg), dimensions(2), size{stres.width, stres.height, 0}, origin{stres.PixelToWorldX(0), stres.PixelToWorldY(0), 0.0}, scale{stres.pixel_scale_x, stres.pixel_scale_y, 1.0}
			{};

		GDALCRS() = default;
		~GDALCRS() = default;
		// Copy
		GDALCRS(const GDALCRS &crs) = default;
		GDALCRS &operator=(const GDALCRS &crs) = default;
		// Move
		GDALCRS(GDALCRS &&lcrs) = default;
		GDALCRS &operator=(GDALCRS &&crs) = default;


		bool operator==(const GDALCRS &b) const;
		bool operator!=(const GDALCRS &b) const { return !(*this == b); }
		size_t getPixelCount() const;
		void verify() const;

		SpatioTemporalReference toSpatioTemporalReference(bool &flipx, bool &flipy, timetype_t timetype, double t1, double t2) const;

		epsg_t epsg;
		uint8_t dimensions; // 1 .. 3
		uint32_t size[3]; // size of the raster in pixels
		double origin[3]; // world coordinates of the outer corner of pixel (0,0)
		double scale[3]; // size of each pixel
		/*
		 * World coordinates of the center of the pixel at (x, y) are:
		 * world_x = origin[0] + (x+0.5) * scale[0]
		 * world_y = origin[1] + (y+0.5) * scale[1]
		 */

	private:
		// These are only meant to be used in RasterSource, thus private

		// These return the world coordinates of the top left corner of the pixel.
		double PixelToWorldX(int px) const { return origin[0] + px * scale[0]; }
		double PixelToWorldY(int py) const { return origin[1] + py * scale[1]; }
		double PixelToWorldZ(int pz) const { return origin[2] + pz * scale[2]; }

		// These do not return fixed pixel coordinates, but doubles. A return value of 0.5 is the center of the first pixel.
		// floor() the results to get the exact pixel the world coordinate belongs in.
		double WorldToPixelX(double wx) const { return (wx - origin[0]) / scale[0]; }
		double WorldToPixelY(double wy) const { return (wy - origin[1]) / scale[1]; }
		double WorldToPixelZ(double wz) const { return (wz - origin[2]) / scale[2]; }


		friend class RasterSource;
		friend std::ostream& operator<< (std::ostream &out, const GDALCRS &crs);
};


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
		std::unique_ptr<GenericRaster> query(const QueryRectangle &rect, QueryProfiler &profiler, int channelid, bool transform = true);

		bool isWriteable() const { return writeable; }

	private:
		void import(GenericRaster *raster, int channelid, time_t timestamp, GenericRaster::Compression compression = GenericRaster::Compression::GZIP);
		void importTile(GenericRaster *raster, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp, GenericRaster::Compression compression = GenericRaster::Compression::PREDICTED);

		bool hasTile(uint32_t width, uint32_t height, uint32_t depth, int offx, int offy, int offz, int zoom, int channelid, time_t timestamp);

		std::unique_ptr<GenericRaster> load(int channelid, time_t timestamp, int x1, int y1, int x2, int y2, int zoom = 0, bool transform = true, size_t *io_cost = nullptr);
		std::unique_ptr<GenericRaster> loadTile(int channelid, int fileid, size_t offset, size_t size, uint32_t width, uint32_t height, uint32_t depth, GenericRaster::Compression method);

		void init();
		void cleanup();

		int lockedfile;
		bool writeable;
		std::string filename_json;
		std::string filename_data;
		std::string filename_db;
		GDALCRS *crs;
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
