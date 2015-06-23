#ifndef OPERATORS_QUERYRECTANGLE_H
#define OPERATORS_QUERYRECTANGLE_H

#include "datatypes/spatiotemporal.h"

class BinaryStream;

class QueryRectangle {
	public:
		QueryRectangle();
		QueryRectangle(time_t timestamp, double x1, double y1, double x2, double y2, uint32_t xres, uint32_t yres, epsg_t epsg) : timestamp(timestamp), x1(std::min(x1,x2)), y1(std::min(y1,y2)), x2(std::max(x1,x2)), y2(std::max(y1,y2)), xres(xres), yres(yres), epsg(epsg) {};
		QueryRectangle(const GridSpatioTemporalResult &grid);
		QueryRectangle(BinaryStream &stream);

		void toStream(BinaryStream &stream) const;

		double minx() const;
		double maxx() const;
		double miny() const;
		double maxy() const;

		void enlarge(int pixels);

		time_t timestamp;
		double x1, y1, x2, y2;
		uint32_t xres, yres;
		epsg_t epsg;
};

#endif
