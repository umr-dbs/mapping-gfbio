
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

void QueryRectangle::enlargePixels(int pixels) {
	if (restype != QueryResolution::Type::PIXELS)
		throw ArgumentException("Cannot enlarge QueryRectangle without a proper pixel size");

	double pixel_size_in_world_coordinates_x = (double) (x2 - x1) / xres;
	double pixel_size_in_world_coordinates_y = (double) (y2 - y1) / yres;

	x1 -= pixels * pixel_size_in_world_coordinates_x;
	x2 += pixels * pixel_size_in_world_coordinates_x;
	y1 -= pixels * pixel_size_in_world_coordinates_y;
	y2 += pixels * pixel_size_in_world_coordinates_y;

	xres += 2*pixels;
	yres += 2*pixels;
}

void QueryRectangle::enlargeFraction(double fraction) {
	// If the desired resolution is specified in pixels, we would need to adjust the amount of requested pixels as well.
	// Until there's a use case for this, I'd rather not bother figuring out the best way to handle rounding.
	if (restype == QueryResolution::Type::PIXELS)
		throw ArgumentException("Cannot (yet) enlarge QueryRectangle by a fraction when a pixel size is present");

	double enlarge_x = (x2 - x1) * fraction;
	double enlarge_y = (y2 - y1) * fraction;

	x1 -= enlarge_x;
	x2 += enlarge_x;

	y1 -= enlarge_y;
	y2 += enlarge_y;
}
