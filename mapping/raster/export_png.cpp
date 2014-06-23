
#include "raster/raster_priv.h"
#include "raster/typejuggling.h"
#include "raster/colors.h"

#include <png.h>
#include <stdio.h>


template<typename T> void Raster2D<T>::toPNG(const char *filename, Colorizer &colorizer, bool flipx, bool flipy) {
	if (!RasterTypeInfo<T>::isinteger)
		throw new MetadataException("toPNG cannot write float rasters");
	if (lcrs.dimensions != 2)
		throw new MetadataException("toPNG can only handle rasters with 2 dimensions");

	this->setRepresentation(GenericRaster::Representation::CPU);

	FILE *file = nullptr;
	if (filename != nullptr) {
		file = fopen(filename, "w");
		if (!file)
			throw ExporterException("Could not write to file");
	}

	T max = dd.max;
	T min = dd.min;
	auto range = RasterTypeInfo<T>::getRange(min, max);

	png_structp png_ptr = png_create_write_struct(
			PNG_LIBPNG_VER_STRING,
			NULL, NULL, NULL
			/*
			(png_voidp) user_error_ptr,
			user_error_fn,
			user_warning_fn
			*/
	);
	if (!png_ptr)
		throw ExporterException("Could not initialize libpng");

	png_infop info_ptr = png_create_info_struct(png_ptr);
	if (!info_ptr) {
		png_destroy_write_struct(&png_ptr, (png_infopp) NULL);
		throw ExporterException("Could not initialize libpng");
	}

	if (filename != nullptr) {
		png_init_io(png_ptr, file);
	}
	else {
		png_init_io(png_ptr, stdout);
	}

	png_set_IHDR(png_ptr, info_ptr,
		lcrs.size[0],  lcrs.size[1],
		8, PNG_COLOR_TYPE_PALETTE,
		PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT
	);

	uint32_t colors[256];
	if (dd.has_no_data) {
		colors[0] = color_from_rgba(0,0,0,0);
		colorizer.setRange(254);
		for (uint32_t i=1;i<256;i++) {
			colors[i] = colorizer.colorize(i-1);
		}
	}
	else {
		colorizer.setRange(255);
		for (uint32_t i=0;i<256;i++) {
			colors[i] = colorizer.colorize(i);
		}
	}

	// transform into png_color array to set PLTE chunk
	png_color colors_rgb[256];
	png_byte colors_a[256];
	for (uint32_t i=0;i<256;i++) {
		uint32_t c = colors[i];
		colors_rgb[i].red   = (c >>  0) & 0xff;
		colors_rgb[i].green = (c >>  8) & 0xff;
		colors_rgb[i].blue  = (c >> 16) & 0xff;
		colors_a[i]         = (c >> 24) & 0xff;
	}
	png_set_PLTE(png_ptr, info_ptr, colors_rgb, 256);
	png_set_tRNS(png_ptr, info_ptr, colors_a, 256, nullptr);


/*	if (bpp == 32) {
		png_set_IHDR(png_ptr, info_ptr,
			lcrs.size[0],  lcrs.size[1],
			8, PNG_COLOR_TYPE_RGB_ALPHA,
			PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT
		);
	}*/

	png_write_info(png_ptr, info_ptr);
	png_set_filter(png_ptr, PNG_FILTER_TYPE_BASE, PNG_FILTER_NONE | PNG_FILTER_PAETH);

	int width = lcrs.size[0];
	int height = lcrs.size[1];
	uint8_t *row = new uint8_t[ width ];
	for (int y=0;y<height;y++) {
		int py = flipy ? height-y : y;
		for (int x=0;x<width;x++) {
			int px = flipx ? width-x : x;
			T v = get(px, py);
			if (dd.has_no_data && v == dd.no_data)
				row[x] = 0;
			else {
				row[x] = round(((float) v - min) / range * 254) + 1;
			}
			if (x == 0 || y == 0 || x == width-1 || y == height-1)
				row[x] = 255;
		}
		png_write_row(png_ptr, (png_bytep) row);
	}
	delete [] row;

/*
	else if (bpp == 32) {
		T nodata = (T) dd.no_data;
		uint32_t *row = new uint32_t[ width ];
		for (int y=0;y<height;y++) {
			int py = flipy ? height-y : y;
			for (int x=0;x<width;x++) {
				int px = flipx ? width-x : x;
				T rawvalue = get(px, py);
				uint32_t value = rawvalue - min;
				if (dd.has_no_data && rawvalue == nodata)
					row[x] = color_from_rgba(0,0,0,0);
				else
					row[x] = colorizer.colorize32(value);
				if (x == 0 || y == 0 || x == width-1 || y == height-1)
					row[x] = color_from_rgba(255,0,0,255); // 0xff0000ff;
			}
			png_write_row(png_ptr, (png_bytep) row);
		}
		delete [] row;
	}
*/

	png_write_end(png_ptr, info_ptr);

	png_destroy_write_struct(&png_ptr, &info_ptr);
	if (filename != nullptr) {
		fclose(file);
	}
}


template<> void Raster2D<float>::toPNG(const char *, Colorizer &, bool, bool) {
	throw ConverterException("toPNG cannot write float rasters");
}

RASTER_PRIV_INSTANTIATE_ALL
