#ifndef CONVERTERS_CONVERTER_H
#define CONVERTERS_CONVERTER_H

#include "raster/raster.h"

class ByteBuffer {
	public:
		ByteBuffer(unsigned char *data, size_t size) : data(data), size(size) {};
		ByteBuffer(size_t size) : data(nullptr), size(size) { data = new unsigned char[size]; }
		~ByteBuffer() { delete [] data; data = nullptr; size = 0; };
		unsigned char *data;
		size_t size;
	private:
		void operator=(ByteBuffer &);
};



class RasterConverter {
	protected:
		RasterConverter(const RasterMetadata &rastermetadata, const ValueMetadata &valuemetadata)
			: rastermeta(rastermetadata), valuemeta(valuemetadata) {};
	public:
		virtual ~RasterConverter() { }

		static ByteBuffer *direct_encode(GenericRaster *raster, GenericRaster::Compression method);
		static GenericRaster *direct_decode(const RasterMetadata &rastermetadata, const ValueMetadata &valuemetadata, ByteBuffer *buffer, GenericRaster::Compression method);

		static RasterConverter *getConverter(const RasterMetadata &rastermetadata, const ValueMetadata &valuemetadata, GenericRaster::Compression method);

		virtual ByteBuffer *encode(GenericRaster *raster) = 0;
		virtual GenericRaster *decode(ByteBuffer *buffer) = 0;


		const RasterMetadata rastermeta;
		const ValueMetadata valuemeta;
};




#endif