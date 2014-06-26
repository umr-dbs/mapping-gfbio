#ifndef RASTER_RASTER_H
#define RASTER_RASTER_H

#include <stdint.h>
#include <gdal_priv.h>

#include <ostream>

#include "raster/exceptions.h"
#include "raster/metadata.h"

#define EPSG_UNKNOWN 0
#define EPSG_METEOSAT2 0xF592  // 62866  // 0xFE1E05A1
#define EPSG_WEBMERCATOR 3857 // 3785 is deprecated
#define EPSG_LATLON 4326 // http://spatialreference.org/ref/epsg/wgs-84/

typedef uint16_t epsg_t;

class QueryRectangle;


// LocalCoordinateSystem - lcs
class LocalCRS {
	public:
		LocalCRS(epsg_t epsg, uint32_t w, double origin_x, double scale_x)
			: epsg(epsg), dimensions(1), size{w, 0, 0}, origin{origin_x, 0, 0}, scale{scale_x, 0, 0} {};

		LocalCRS(epsg_t epsg, uint32_t w, uint32_t h, double origin_x, double origin_y, double scale_x, double scale_y)
			: epsg(epsg), dimensions(2), size{w, h, 0}, origin{origin_x, origin_y, 0}, scale{scale_x, scale_y, 0} {};

		LocalCRS(epsg_t epsg, uint32_t w, uint32_t h, uint32_t d, double origin_x, double origin_y, double origin_z, double scale_x, double scale_y, double scale_z)
			: epsg(epsg), dimensions(3), size{w, h, d}, origin{origin_x, origin_y, origin_z}, scale{scale_x, scale_y, scale_z} {};

		LocalCRS(epsg_t epsg, int dimensions, uint32_t w, uint32_t h, uint32_t d, double origin_x, double origin_y, double origin_z, double scale_x, double scale_y, double scale_z)
			: epsg(epsg), dimensions(dimensions), size{w, h, d}, origin{origin_x, origin_y, origin_z}, scale{scale_x, scale_y, scale_z} {};

		LocalCRS(const QueryRectangle &rect);

		LocalCRS() = delete;
		~LocalCRS() = default;
		// Copy
		LocalCRS(const LocalCRS &lcrs) = default;
		LocalCRS &operator=(const LocalCRS &lcrs) = default;
		// Move
		LocalCRS(LocalCRS &&lcrs) = default;
		LocalCRS &operator=(LocalCRS &&lcrs) = default;


		bool operator==(const LocalCRS &b) const;
		size_t getPixelCount() const;
		void verify() const;

		double PixelToWorldX(int x) const { return origin[0] + x * scale[0]; }
		double PixelToWorldY(int y) const { return origin[1] + y * scale[1]; }
		double PixelToWorldZ(int z) const { return origin[2] + z * scale[2]; }

		double WorldToPixelX(double wx) const { return (wx - origin[0]) / scale[0]; }
		double WorldToPixelY(double wy) const { return (wy - origin[1]) / scale[1]; }
		double WorldToPixelZ(double wz) const { return (wz - origin[2]) / scale[2]; }

		epsg_t epsg;
		uint8_t dimensions; // 1 .. 3
		uint32_t size[3]; // size of the raster in pixels
		double origin[3]; // world coordinates of the point at pixel coordinates (0,0)
		double scale[3]; // size of each pixel
		/*
		 * World coordinates of the pixel at (x, y) are:
		 * world_x = origin[0] + x * scale[0]
		 * world_y = origin[1] + y * scale[1]
		 */

		friend std::ostream& operator<< (std::ostream &out, const LocalCRS &lcrs);
};


class DataDescription {
	public:
		DataDescription(GDALDataType datatype, double min, double max)
			: datatype(datatype), min(min), max(max), has_no_data(false), no_data(0.0) {};

		DataDescription(GDALDataType datatype, double min, double max, bool has_no_data, double no_data)
			: datatype(datatype), min(min), max(max), has_no_data(has_no_data), no_data(has_no_data ? no_data : 0.0) {};

		DataDescription() = delete;
		~DataDescription() = default;
		// Copy
		DataDescription(const DataDescription &dd) = default;
		DataDescription &operator=(const DataDescription &dd) = delete;
		// Move
		DataDescription(DataDescription &&dd) = delete;
		DataDescription &operator=(DataDescription &&dd) = delete;

		void addNoData();

		bool operator==(const DataDescription &b) const;
		void verify() const;
		int getBPP() const; // Bytes per Pixel
		double getMinByDatatype() const;
		double getMaxByDatatype() const;

		void print() const;
		friend std::ostream& operator<< (std::ostream &out, const DataDescription &dd);

		GDALDataType datatype;
		double min, max;
		bool has_no_data;
		double no_data;
};

namespace cl {
	class Buffer;
}

class Colorizer;

class GenericRaster {
	public:
		enum Representation {
			CPU = 1,
			OPENCL = 2
		};

		enum Compression {
			UNCOMPRESSED = 1,
			BZIP = 2,
			PREDICTED = 3,
			GZIP = 4
		};

		virtual void setRepresentation(Representation r) = 0;
		Representation getRepresentation() const { return representation; }

		static GenericRaster *create(const LocalCRS &localcrs, const DataDescription &datadescription, Representation representation = Representation::CPU);
		static GenericRaster *fromGDAL(const char *filename, int rasterid, epsg_t epsg = EPSG_UNKNOWN);

		virtual ~GenericRaster();
		GenericRaster(const GenericRaster &) = delete;
		GenericRaster &operator=(const GenericRaster &) = delete;

		virtual void toPGM(const char *filename, bool avg = false) = 0;
		virtual void toYUV(const char *filename) = 0;
		virtual void toPNG(const char *filename, Colorizer &colorizer, bool flipx = false, bool flipy = false) = 0;
		virtual void toJPEG(const char *filename, Colorizer &colorizer, bool flipx = false, bool flipy = false) = 0;
		virtual void toGDAL(const char *filename, const char *driver) = 0;

		virtual const void *getData() = 0;
		virtual size_t getDataSize() = 0;
		virtual cl::Buffer *getCLBuffer() = 0;
		virtual cl::Buffer *getCLInfoBuffer() = 0;
		virtual void *getDataForWriting() = 0;
		virtual int getBPP() = 0; // Bytes per Pixel
		virtual double getAsDouble(int x, int y=0, int z=0) = 0;

		virtual void clear(double value) = 0;
		virtual void blit(const GenericRaster *raster, int x, int y=0, int z=0) = 0;
		virtual GenericRaster *cut(int x, int y, int z, int width, int height, int depths) = 0;
		GenericRaster *cut(int x, int y, int width, int height) { return cut(x,y,0,width,height,0); }
		virtual GenericRaster *scale(int width, int height=0, int depth=0) = 0;

		std::string hash();


		const LocalCRS lcrs;
		const DataDescription dd;

		DirectMetadata<std::string> md_string;
		DirectMetadata<double> md_value;

	protected:
		GenericRaster(const LocalCRS &localcrs, const DataDescription &datadescription);
		Representation representation;
};

#endif
