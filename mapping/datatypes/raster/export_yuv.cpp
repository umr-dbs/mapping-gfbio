
#include "datatypes/raster/raster_priv.h"

#include <stdio.h>

template<typename T> void Raster2D<T>::toYUV(const char *filename) {
	FILE *file = fopen(filename, "w");
	if (!file)
		throw ExporterException("Could not write to file");

	if (lcrs.size[0]%2 || lcrs.size[1]%2)
		throw new ExporterException("YUV420 needs even width and height");

	int width = lcrs.size[0];
	int height = lcrs.size[1];
	// YUV420p wie in wikipedia definiert
	// kein Header

	// Y in voller Auflösung
	for (int y=0;y<height;y++) {
		for (int x=0;x<width;x++) {
			fprintf(file, "%c", (unsigned char) (256.0*get(x, y)/dd.max));
		}
	}

	// U (Cb) in halber Auflösung
	for (int y=0;y<height;y+=2) {
		for (int x=0;x<width;x+=2) {
			fprintf(file, "%c", (unsigned char) 128);
		}
	}

	// V (Cr) in halber Auflösung
	for (int y=0;y<height;y+=2) {
		for (int x=0;x<width;x+=2) {
			fprintf(file, "%c", (unsigned char) 128);
		}
	}

	fclose(file);
}


RASTER_PRIV_INSTANTIATE_ALL
