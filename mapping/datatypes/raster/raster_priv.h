#ifndef RASTER_RASTER_PRIV_H
#define RASTER_RASTER_PRIV_H 1

#include "datatypes/raster.h"

template<typename T, int dimensions> class Raster : public GenericRaster {
	public:
		Raster(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth);
		virtual ~Raster();

		virtual size_t getDataSize() { return sizeof(T)*getPixelCount(); };
		virtual int getBPP() { return sizeof(T); }

		virtual void setRepresentation(Representation);

		virtual const void *getData() { setRepresentation(GenericRaster::Representation::CPU); return (void *) data; };
		virtual void *getDataForWriting() { setRepresentation(GenericRaster::Representation::CPU); return (void *) data; };

		virtual cl::Buffer *getCLBuffer() { return clbuffer; };
		virtual cl::Buffer *getCLInfoBuffer() { return clbuffer_info; };

	protected:
		T *data;
		void *clhostptr;
		cl::Buffer *clbuffer;
		cl::Buffer *clbuffer_info;
};

class RasterOperator;

template<typename T> class Raster2D : public Raster<T, 2> {
	public:
		Raster2D(const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth = 0)
			:Raster<T, 2>(datadescription, stref, width, height, depth) {};
		virtual ~Raster2D();

		virtual void toPGM(const char *filename, bool avg);
		virtual void toYUV(const char *filename);
		virtual void toPNG(const char *filename, const Colorizer &colorizer, bool flipx = false, bool flipy = false, Raster2D<uint8_t> *overlay = nullptr);
		virtual void toJPEG(const char *filename, const Colorizer &colorizer, bool flipx = false, bool flipy = false);
		virtual void toGDAL(const char *filename, const char *driver, bool flipx = false, bool flipy = false);

		virtual void clear(double value);
		virtual void blit(const GenericRaster *raster, int x, int y=0, int z=0);
		virtual std::unique_ptr<GenericRaster> cut(int x, int y, int z, int width, int height, int depths);
		virtual std::unique_ptr<GenericRaster> scale(int width, int height=0, int depth=0);
		virtual std::unique_ptr<GenericRaster> flip(bool flipx, bool flipy);
		virtual std::unique_ptr<GenericRaster> fitToQueryRectangle(const QueryRectangle &qrect);
		virtual void print(int x, int y, double value, const char *text, int maxlen = -1);

		virtual double getAsDouble(int x, int y=0, int z=0) const;

		T get(int x, int y) const {
			return data[(size_t) y*width + x];
		}
		T getSafe(int x, int y, T def = 0) const {
			if (x >= 0 && y >= 0 && (uint32_t) x < width && (uint32_t) y < height)
				return data[(size_t) y*width + x];
			return def;
		}
		void set(int x, int y, T value) {
			data[(size_t) y*width + x] = value;
		}
		void setSafe(int x, int y, T value) {
			if (x >= 0 && y >= 0 && (uint32_t) x < width && (uint32_t) y < height)
				data[(size_t) y*width + x] = value;
		}

		// create aliases for parent classes members
		// otherwise we'd need to write this->data every time
		// see: two-phase lookup of dependant names
	public:
		using Raster<T, 2>::stref;
		using Raster<T, 2>::width;
		using Raster<T, 2>::height;
		using Raster<T, 2>::pixel_scale_x;
		using Raster<T, 2>::pixel_scale_y;
		using Raster<T, 2>::PixelToWorldX;
		using Raster<T, 2>::PixelToWorldY;
		using Raster<T, 2>::WorldToPixelX;
		using Raster<T, 2>::WorldToPixelY;
		using Raster<T, 2>::data;
		using Raster<T, 2>::dd;
		using Raster<T, 2>::getPixelCount;
		using Raster<T, 2>::getDataSize;
		using Raster<T, 2>::setRepresentation;
};


#define RASTER_PRIV_INSTANTIATE_ALL template class Raster2D<uint8_t>;template class Raster2D<uint16_t>;template class Raster2D<int16_t>;template class Raster2D<uint32_t>;template class Raster2D<int32_t>;template class Raster2D<float>;template class Raster2D<double>;



#endif
