
#include "operators/queryrectangle.h"
#include "util/binarystream.h"
#include "util/exceptions.h"

#include <algorithm>


/*
 * QueryResolution class
 */

QueryResolution::QueryResolution(BinaryStream &stream) {
	stream.read(&restype);
	stream.read(&xres);
	stream.read(&yres);
}

void QueryResolution::toStream(BinaryStream &stream) const {
	stream.write(restype);
	stream.write(xres);
	stream.write(yres);
}

/*
 * QueryRectangle class
 */
QueryRectangle::QueryRectangle(const GridSpatioTemporalResult &grid)
	: QueryRectangle(grid.stref, grid.stref, QueryResolution::pixels(grid.width, grid.height)) {
}


QueryRectangle::QueryRectangle(BinaryStream &stream) : SpatialReference(stream), TemporalReference(stream), QueryResolution(stream) {
}

void QueryRectangle::toStream(BinaryStream &stream) const {
	SpatialReference::toStream(stream);
	TemporalReference::toStream(stream);
	QueryResolution::toStream(stream);
}

void QueryRectangle::enlarge(int pixels) {
	if (restype != QueryResolution::Type::PIXELS)
		throw ArgumentException("Cannot enlarge QueryRectangle without a proper pixel size");

	double pixel_size_in_world_coordinates_x = (double) std::abs(x2 - x1) / xres;
	double pixel_size_in_world_coordinates_y = (double) std::abs(y2 - y1) / yres;

	x1 -= pixels * pixel_size_in_world_coordinates_x;
	x2 += pixels * pixel_size_in_world_coordinates_x;
	y1 -= pixels * pixel_size_in_world_coordinates_y;
	y2 += pixels * pixel_size_in_world_coordinates_y;

	xres += 2*pixels;
	yres += 2*pixels;
}
