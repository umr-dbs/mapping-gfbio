
#include "datatypes/raster/raster_priv.h"
#include "datatypes/raster/typejuggling.h"
#include "datatypes/colorizer.h"

#include <png.h>
#include <stdio.h>
#include <sstream>
#include <cmath>


static void png_write_wrapper(png_structp png_ptr, png_bytep data, png_size_t length) {
	std::ostream *stream = (std::ostream *) png_get_io_ptr(png_ptr);
	stream->write((const char *) data, length);
}


template<typename T> void Raster2D<T>::toPNG(std::ostream &output, const Colorizer &colorizer, bool flipx, bool flipy, Raster2D<uint8_t> *overlay) {
	this->setRepresentation(GenericRaster::Representation::CPU);

	if (overlay) {
		// do not use the overlay if the size does not match
		if (overlay->width != width || overlay->height != height)
			overlay = nullptr;
	}

	if (overlay) {
		// Write debug info
		std::ostringstream msg_scale;
		msg_scale.precision(2);
		msg_scale << std::fixed << "scale: " << pixel_scale_x << ", " << pixel_scale_y;
		overlay->print(4, 26, 1, msg_scale.str().c_str());

		std::ostringstream msg_unit;
		msg_unit << "Unit: " << dd.unit.getMeasurement() << ", " << dd.unit.getUnit();
		overlay->print(4, 36, 1, msg_unit.str().c_str());
	}

	T max = dd.unit.getMax();
	T min = dd.unit.getMin();

	T actual_min = min;
	T actual_max = max;
	if (colorizer.is_absolute) {
		// calculate the actual min/max so we can include only the range we require
		actual_min = (T) dd.getMaxByDatatype();
		actual_max = (T) dd.getMinByDatatype();
		bool found_pixel = false;
		auto size = getPixelCount();
		for (size_t i=0;i<size;i++) {
			T v = data[i];
			if (dd.is_no_data(v))
				continue;
			actual_min = std::min(actual_min, v);
			actual_max = std::max(actual_max, v);
			found_pixel = true;
		}
		if (!found_pixel) {
			actual_min = 0;
			actual_max = 1;
		}
	}

	if (!std::isfinite(actual_min) || !std::isfinite(actual_max))
		throw ExporterException("Cannot export PNG without either a known min/max or an absolute colorizer");

	//auto actual_range = RasterTypeInfo<T>::getRange(actual_min, actual_max);

	uint32_t colors[256];
	colors[0] = color_from_rgba(0,0,0,0);
	colors[1] = color_from_rgba(255,0,255,255);
	colorizer.fillPalette(&colors[2], 254, actual_min, actual_max);

	if (overlay) {
		std::ostringstream msg;
		msg << GDALGetDataTypeName(dd.datatype) << " (" << (double) actual_min << " - " << (double) actual_max << ")";
		overlay->print(4, 16, 1, msg.str().c_str());
	}


	// prepare PNG output
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

	// write into the provided ostream
	png_set_write_fn(png_ptr, &output, png_write_wrapper, nullptr);

	// start PNG output, headers first
	png_set_IHDR(png_ptr, info_ptr,
		width, height,
		8, PNG_COLOR_TYPE_PALETTE,
		PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT
	);

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

	uint8_t *row = new uint8_t[ width ];
	for (uint32_t y=0;y<height;y++) {
		uint32_t py = flipy ? height-y-1 : y;
		for (uint32_t x=0;x<width;x++) {
			uint32_t px = flipx ? width-x-1 : x;
			T v = get(px, py);
			if (overlay && overlay->get(x, y) == 1) {
				row[x] = 1;
			}
			else if (dd.is_no_data(v)) {
				row[x] = 0;
			}
			else if (v < actual_min || v > actual_max) {
				row[x] = 1;
			}
			else {
				if (actual_min == actual_max)
					row[x] = 3;
				else
					row[x] = round(253.0 * ((float) v - actual_min) / (actual_max - actual_min)) + 2;
			}
			if (overlay && row[x] != 1) {
				// calculate the distance to the closest image border
				int distx = std::min(x, width-1 - x);
				int disty = std::min(y, height-1 - y);
				if (distx == 0 && (disty < 32 || disty > height/2-16))
					row[x] = 1;
				else if (disty == 0 && (distx < 32 || distx > width/2-16))
					row[x] = 1;
			}

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
}


RASTER_PRIV_INSTANTIATE_ALL
