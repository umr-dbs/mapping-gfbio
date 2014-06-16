
#include "raster/raster_priv.h"
#include "raster/typejuggling.h"

#include <stdio.h>

template<typename T> void Raster2D<T>::toPGM(const char *filename, bool avg) {
	if (rastermeta.dimensions != 2)
		throw new MetadataException("toPGM can only handle rasters with 2 dimensions");

	this->setRepresentation(GenericRaster::Representation::CPU);

	FILE *file = fopen(filename, "w");
	if (!file)
		throw ExporterException("Could not write to file");

	T max = valuemeta.max;
	T min = valuemeta.min;
	auto range = RasterTypeInfo<T>::getRange(min, max);

	fprintf(file, "P2\n%d %d\n%lu\n", rastermeta.size[0], rastermeta.size[1], (uint64_t) range);

	int width = rastermeta.size[0];
	int height = rastermeta.size[1];
	for (int y=0;y<height;y++) {
		for (int x=0;x<width;x++) {
			T value = get(x,y) - min; //(unsigned char) ((raster->getAsDouble(x, y))/max*256);
			if (avg) {
				value = (int) (value + (range/2)) % (int) range;
			}
			fprintf(file, "%u ", (int) value);
		}
		fprintf(file, "\n");
	}
	fclose(file);
}

template<> void Raster2D<float>::toPGM(const char *, bool) {
	throw ConverterException("No PGM export for floats\n");
}

RASTER_PRIV_INSTANTIATE_ALL
