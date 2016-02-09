#ifndef CONVERTERS_CONVERTER_H
#define CONVERTERS_CONVERTER_H

#include "datatypes/raster.h"

class ByteBuffer {
	public:
		ByteBuffer(char *data, size_t size) : data(data), size(size) {};
		ByteBuffer(size_t size) : data(nullptr), size(size) { data = new char[size]; }
		~ByteBuffer() { delete [] data; data = nullptr; size = 0; };
		char *data;
		size_t size;
	private:
		void operator=(ByteBuffer &);
};



class RasterConverter {
	protected:
		RasterConverter() {};
	public:
		virtual ~RasterConverter() { }

		enum Compression {
			UNCOMPRESSED = 1,
			BZIP = 2,
			PREDICTED = 3,
			GZIP = 4
		};

		static std::unique_ptr<ByteBuffer> direct_encode(GenericRaster *raster, RasterConverter::Compression method);
		static std::unique_ptr<GenericRaster> direct_decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, RasterConverter::Compression method);

		static std::unique_ptr<RasterConverter> getConverter(RasterConverter::Compression method);

		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster) = 0;
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth) = 0;
};




#endif
