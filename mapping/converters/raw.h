#ifndef CONVERTERS_RAW_H
#define CONVERTERS_RAW_H


class RawConverter : public RasterConverter {
	public:
		RawConverter(const RasterMetadata &rm, const ValueMetadata &vm);
		virtual ByteBuffer *encode(GenericRaster *raster);
		virtual GenericRaster *decode(ByteBuffer *buffer);
};


class BzipConverter : public RasterConverter {
	public:
		BzipConverter(const RasterMetadata &rm, const ValueMetadata &vm);
		virtual ByteBuffer *encode(GenericRaster *raster);
		virtual GenericRaster *decode(ByteBuffer *buffer);
};

class GzipConverter : public RasterConverter {
	public:
		GzipConverter(const RasterMetadata &rm, const ValueMetadata &vm);
		virtual ByteBuffer *encode(GenericRaster *raster);
		virtual GenericRaster *decode(ByteBuffer *buffer);
};

#endif