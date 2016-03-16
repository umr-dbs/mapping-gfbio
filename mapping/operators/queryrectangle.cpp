
#include "operators/queryrectangle.h"
#include "util/exceptions.h"
#include "util/binarystream.h"

#include <algorithm>

/*
 * QueryResolution class
 */

QueryResolution::QueryResolution(BinaryReadBuffer &buffer) {
	buffer.read(&restype);
	buffer.read(&xres);
	buffer.read(&yres);
}

void QueryResolution::serialize(BinaryWriteBuffer &buffer, bool) const {
	buffer << restype;
	buffer << xres << yres;
}

/*
 * QueryRectangle class
 */
QueryRectangle::QueryRectangle(const GridSpatioTemporalResult &grid)
	: QueryRectangle(grid.stref, grid.stref, QueryResolution::pixels(grid.width, grid.height)) {
}


QueryRectangle::QueryRectangle(BinaryReadBuffer &buffer) : SpatialReference(buffer), TemporalReference(buffer), QueryResolution(buffer) {
}

void QueryRectangle::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	SpatialReference::serialize(buffer, is_persistent_memory);
	TemporalReference::serialize(buffer, is_persistent_memory);
	QueryResolution::serialize(buffer, is_persistent_memory);
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
