#ifndef RASTER_RASTER_PRIV_H
#define RASTER_RASTER_PRIV_H 1

#include "raster/raster.h"

template<typename T, int dimensions> class Raster : public GenericRaster {
	public:
		Raster(const LocalCRS &localcrs, const DataDescription &datadescription);
		virtual ~Raster();

		virtual size_t getDataSize() { return sizeof(T)*lcrs.getPixelCount(); };
		virtual int getBPP() { return sizeof(T); }

		virtual void setRepresentation(Representation);

		virtual const void *getData() { setRepresentation(GenericRaster::Representation::CPU); return (void *) data; };
		virtual void *getDataForWriting() { setRepresentation(GenericRaster::Representation::CPU); return (void *) data; };

		virtual cl::Buffer *getCLBuffer() { return clbuffer; };
		virtual cl::Buffer *getCLInfoBuffer() { return clbuffer_info; };

	protected:
		T *data;
		cl::Buffer *clbuffer;
		cl::Buffer *clbuffer_info;
};

class RasterOperator;

template<typename T> class Raster2D : public Raster<T, 2> {
	public:
		Raster2D(const LocalCRS &localcrs, const DataDescription &datadescription)
			:Raster<T, 2>(localcrs, datadescription) {};
		virtual ~Raster2D();

		virtual void toPGM(const char *filename, bool avg);
		virtual void toYUV(const char *filename);
		virtual void toPNG(const char *filename, const Colorizer &colorizer, bool flipx = false, bool flipy = false, Raster2D<uint8_t> *overlay = nullptr);
		virtual void toJPEG(const char *filename, const Colorizer &colorizer, bool flipx = false, bool flipy = false);
		virtual void toGDAL(const char *filename, const char *driver);

		virtual void clear(double value);
		virtual void blit(const GenericRaster *raster, int x, int y=0, int z=0);
		virtual std::unique_ptr<GenericRaster> cut(int x, int y, int z, int width, int height, int depths);
		virtual std::unique_ptr<GenericRaster> scale(int width, int height=0, int depth=0);
		virtual std::unique_ptr<GenericRaster> flip(bool flipx, bool flipy);
		virtual void print(int x, int y, double value, const char *text, int maxlen = -1);

		virtual double getAsDouble(int x, int y=0, int z=0) const;

		T get(int x, int y) const {
			return data[(size_t) y*this->lcrs.size[0] + x];
		}
		T getSafe(int x, int y, T def = 0) const {
			if (x >= 0 && y >= 0 && (uint32_t) x < lcrs.size[0] && (uint32_t) y < lcrs.size[1])
				return data[(size_t) y*this->lcrs.size[0] + x];
			//fprintf(stderr, "getSafe(%d, %d) outside of range (%u, %u)\n", x, y, lcrs.size[0], lcrs.size[1]);
			return def;
		}
		void set(int x, int y, T value) {
			data[(size_t) y*lcrs.size[0] + x] = value;
		}
		void setSafe(int x, int y, T value) {
			if (x >= 0 && y >= 0 && (uint32_t) x < lcrs.size[0] && (uint32_t) y < lcrs.size[1])
				data[(size_t) y*lcrs.size[0] + x] = value;
		}

		// create aliases for parent classes members
		// otherwise we'd need to write this->data every time
		// see: two-phase lookup of dependant names
	public:
		using Raster<T, 2>::data;
		using Raster<T, 2>::lcrs;
		using Raster<T, 2>::dd;
		using Raster<T, 2>::getDataSize;
		using Raster<T, 2>::setRepresentation;
};


#define RASTER_PRIV_INSTANTIATE_ALL template class Raster2D<uint8_t>;template class Raster2D<uint16_t>;template class Raster2D<int16_t>;template class Raster2D<uint32_t>;template class Raster2D<int32_t>;template class Raster2D<float>;



#endif
