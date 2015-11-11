
#include "datatypes/raster/raster_priv.h"

#include <stdio.h>

template<typename T> void Raster2D<T>::toYUV(const char *filename) {
	if (!dd.unit.hasMinMax())
		throw ConverterException("Cannot export as YUV because the unit does not have finite min/max");

	FILE *file = fopen(filename, "w");
	if (!file)
		throw ExporterException("Could not write to file");

	if (width%2 || height%2)
		throw new ExporterException("YUV420 needs even width and height");

	// YUV420p wie in wikipedia definiert
	// kein Header

	// Y in voller Auflösung
	for (uint32_t y=0;y<height;y++) {
		for (uint32_t x=0;x<width;x++) {
			fprintf(file, "%c", (unsigned char) (256.0*get(x, y)/dd.unit.getMax()));
		}
	}

	// U (Cb) in halber Auflösung
	for (uint32_t y=0;y<height;y+=2) {
		for (uint32_t x=0;x<width;x+=2) {
			fprintf(file, "%c", (unsigned char) 128);
		}
	}

	// V (Cr) in halber Auflösung
	for (uint32_t y=0;y<height;y+=2) {
		for (uint32_t x=0;x<width;x+=2) {
			fprintf(file, "%c", (unsigned char) 128);
		}
	}

	fclose(file);
}


RASTER_PRIV_INSTANTIATE_ALL
