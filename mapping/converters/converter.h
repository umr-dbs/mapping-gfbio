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
		RasterConverter(const LocalCRS &localcrs, const DataDescription &datadescription)
			: localcrs(localcrs), datadescription(datadescription) {};
	public:
		virtual ~RasterConverter() { }

		static std::unique_ptr<ByteBuffer> direct_encode(GenericRaster *raster, GenericRaster::Compression method);
		static std::unique_ptr<GenericRaster> direct_decode(const LocalCRS &rastermetadata, const DataDescription &valuemetadata, ByteBuffer *buffer, GenericRaster::Compression method);

		static std::unique_ptr<RasterConverter> getConverter(const LocalCRS &rastermetadata, const DataDescription &valuemetadata, GenericRaster::Compression method);

		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster) = 0;
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer *buffer) = 0;

		const LocalCRS localcrs;
		const DataDescription datadescription;
};




#endif
