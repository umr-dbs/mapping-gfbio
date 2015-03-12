#ifndef CONVERTERS_RAW_H
#define CONVERTERS_RAW_H


class RawConverter : public RasterConverter {
	public:
		RawConverter();
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth);
};


class BzipConverter : public RasterConverter {
	public:
		BzipConverter();
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth);
};

class GzipConverter : public RasterConverter {
	public:
		GzipConverter();
		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster);
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth);
};

#endif
