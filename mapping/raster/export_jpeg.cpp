
#include "raster/raster_priv.h"
#include "raster/typejuggling.h"
#include "raster/colors.h"


#include <jpeglib.h>
#include <stdio.h>


// adopted from http://svn.code.sf.net/p/libjpeg-turbo/code/trunk/example.c
// TODO: Error handling
template<typename T> void Raster2D<T>::toJPEG(const char *filename, Colorizer &colorizer, bool flipx, bool flipy) {
	const int quality = 95;

	if (!RasterTypeInfo<T>::isinteger)
		throw new MetadataException("toJPEG cannot write float rasters");
	if (rastermeta.dimensions != 2)
		throw new MetadataException("toJPEG can only handle rasters with 2 dimensions");

	this->setRepresentation(GenericRaster::Representation::CPU);

	FILE *file = nullptr;
	if (filename != nullptr) {
		file = fopen(filename, "w");
		if (!file)
			throw ExporterException("Could not write to file");
	}

	T max = valuemeta.max;
	T min = valuemeta.min;
	auto range = RasterTypeInfo<T>::getRange(min, max);

	colorizer.setRange(range);

	struct jpeg_compress_struct cinfo;
	struct jpeg_error_mgr jerr;
	JSAMPROW row_pointer[1];

	cinfo.err = jpeg_std_error(&jerr);
	jpeg_create_compress(&cinfo);

	jpeg_stdio_dest(&cinfo, filename != nullptr ? file : stdout);

	int width = rastermeta.size[0];
	int height = rastermeta.size[1];

	cinfo.image_width = width;
	cinfo.image_height = height;
	/*
	if (bpp == 8) {
		cinfo.input_components = 1;
		cinfo.in_color_space = JCS_GRAYSCALE; // I wish we had alpha. Oh well.
	}
	else if (bpp == 32) */ {
		cinfo.input_components = 3;
		cinfo.in_color_space = JCS_RGB; // I wish we had alpha. Oh well.
	}

	jpeg_set_defaults(&cinfo);

	jpeg_set_quality(&cinfo, quality, TRUE /* limit to baseline-JPEG values */);


	jpeg_start_compress(&cinfo, TRUE);

	/*if (bpp == 8) {
		int row_stride = width;
		JSAMPLE *row = new JSAMPLE[ row_stride ];
		while (cinfo.next_scanline < cinfo.image_height) {
			int y = cinfo.next_scanline;
			int py = flipy ? height-y : y;
			for (int x=0;x<width;x++) {
				int px = flipx ? width-x : x;
				JSAMPLE color = colorizer.colorize8(get(px, py) - valuemeta.min);
				if (x == 0 || y == 0 || x == width-1 || y == height-1)
					color = 255;
				row[x] = color;
			}
			row_pointer[0] = row;
			(void) jpeg_write_scanlines(&cinfo, row_pointer, 1);
		}
		delete [] row;
	}
	else if (bpp == 32) */{
		int row_stride = width * 3;
		JSAMPLE *row = new JSAMPLE[ row_stride ];
		while (cinfo.next_scanline < cinfo.image_height) {
			int y = cinfo.next_scanline;
			int py = flipy ? height-y : y;
			for (int x=0;x<width;x++) {
				int px = flipx ? width-x : x;
				uint32_t color = colorizer.colorize(get(px, py) - valuemeta.min);
				if (x == 0 || y == 0 || x == width-1 || y == height-1)
					color = color_from_rgba(255,0,0,255);
				row[3*x  ] = (color >>  0) & 0xff; // red
				row[3*x+1] = (color >>  8) & 0xff; // green
				row[3*x+2] = (color >> 16) & 0xff; // blue
				// no alpha :(
			}
			row_pointer[0] = row;
			(void) jpeg_write_scanlines(&cinfo, row_pointer, 1);
		}
		delete [] row;
	}

	jpeg_finish_compress(&cinfo);

	if (filename != nullptr)
		fclose(file);

	jpeg_destroy_compress(&cinfo);
}


template<> void Raster2D<float>::toJPEG(const char *, Colorizer &, bool, bool) {
	throw ConverterException("toJPEG cannot write float rasters");
}

RASTER_PRIV_INSTANTIATE_ALL
