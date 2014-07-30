#ifndef CONVERTERS_RAW_H
#define CONVERTERS_RAW_H


class RawConverter : public RasterConverter {
	public:
		RawConverter(const LocalCRS &rm, const DataDescription &vm);
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer *buffer);
};


class BzipConverter : public RasterConverter {
	public:
		BzipConverter(const LocalCRS &rm, const DataDescription &vm);
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer *buffer);
};

class GzipConverter : public RasterConverter {
	public:
		GzipConverter(const LocalCRS &rm, const DataDescription &vm);
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer *buffer);
};

#endif
