#ifndef RASTERDB_CONVERTERS_CONVERTER_H
#define RASTERDB_CONVERTERS_CONVERTER_H

#include "datatypes/raster.h"
#include "util/make_unique.h"


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

		static std::unique_ptr<ByteBuffer> direct_encode(GenericRaster *raster, const std::string &method);
		static std::unique_ptr<GenericRaster> direct_decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth, const std::string &method);

		static std::unique_ptr<RasterConverter> getConverter(const std::string &method);

		virtual std::unique_ptr<ByteBuffer> encode(GenericRaster *raster) = 0;
		virtual std::unique_ptr<GenericRaster> decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth) = 0;
};


class RasterConverterRegistration {
	public:
		RasterConverterRegistration(const char *name, std::unique_ptr<RasterConverter> (*constructor)());
};

#define REGISTER_RASTERCONVERTER(classname, name) static std::unique_ptr<RasterConverter> create##classname() { return make_unique<classname>(); } static RasterConverterRegistration register_##classname(name, create##classname)



#endif
