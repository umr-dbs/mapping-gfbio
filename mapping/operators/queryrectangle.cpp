
#include "operators/queryrectangle.h"
#include "util/binarystream.h"

#include <algorithm>


/*
 * QueryRectangle class
 */
QueryRectangle::QueryRectangle(const GridSpatioTemporalResult &grid)
	: QueryRectangle(grid.stref.t1, grid.stref.x1, grid.stref.y1, grid.stref.x2, grid.stref.y2, grid.width, grid.height, grid.stref.epsg){
}


QueryRectangle::QueryRectangle(BinaryStream &socket) {
	socket.read(&timestamp);
	socket.read(&x1);
	socket.read(&y1);
	socket.read(&x2);
	socket.read(&y2);
	socket.read(&xres);
	socket.read(&yres);
	socket.read(&epsg);
}

void QueryRectangle::toStream(BinaryStream &stream) const {
	stream.write(timestamp);
	stream.write(x1);
	stream.write(y1);
	stream.write(x2);
	stream.write(y2);
	stream.write(xres);
	stream.write(yres);
	stream.write(epsg);
}



double QueryRectangle::minx() const { return std::min(x1, x2); }
double QueryRectangle::maxx() const { return std::max(x1, x2); }
double QueryRectangle::miny() const { return std::min(y1, y2); }
double QueryRectangle::maxy() const { return std::max(y1, y2); }


void QueryRectangle::enlarge(int pixels) {
	double pixel_size_in_world_coordinates_x = (double) std::abs(x2 - x1) / xres;
	double pixel_size_in_world_coordinates_y = (double) std::abs(y2 - y1) / yres;

	x1 -= pixels * pixel_size_in_world_coordinates_x;
	x2 += pixels * pixel_size_in_world_coordinates_x;
	y1 -= pixels * pixel_size_in_world_coordinates_y;
	y2 += pixels * pixel_size_in_world_coordinates_y;

	xres += 2*pixels;
	yres += 2*pixels;
}
