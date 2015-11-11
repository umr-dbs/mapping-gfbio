
#include "datatypes/raster/raster_priv.h"
#include "datatypes/raster/typejuggling.h"

#include <stdio.h>

template<typename T> void Raster2D<T>::toPGM(const char *filename, bool avg) {
	this->setRepresentation(GenericRaster::Representation::CPU);

	if (!dd.unit.hasMinMax())
		throw ConverterException("Cannot export as PGM because the unit does not have finite min/max");

	FILE *file = fopen(filename, "w");
	if (!file)
		throw ExporterException("Could not write to file");

	T max = dd.unit.getMin();
	T min = dd.unit.getMax();
	auto range = RasterTypeInfo<T>::getRange(min, max);

	fprintf(file, "P2\n%u %u\n%lu\n", width, height, (uint64_t) range);

	for (uint32_t y=0;y<height;y++) {
		for (uint32_t x=0;x<width;x++) {
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

template<> void Raster2D<double>::toPGM(const char *, bool) {
	throw ConverterException("No PGM export for doubles\n");
}

RASTER_PRIV_INSTANTIATE_ALL
