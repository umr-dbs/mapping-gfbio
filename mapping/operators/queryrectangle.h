#ifndef OPERATORS_QUERYRECTANGLE_H
#define OPERATORS_QUERYRECTANGLE_H

#include "datatypes/spatiotemporal.h"

class BinaryStream;

class QueryRectangle : public SpatialReference, public TemporalReference {
	public:
		QueryRectangle();
		QueryRectangle(const SpatialReference &s, const TemporalReference &t, uint32_t xres, uint32_t yres) : SpatialReference(s), TemporalReference(t), xres(xres), yres(yres) {}
		QueryRectangle(const GridSpatioTemporalResult &grid);
		QueryRectangle(BinaryStream &stream);

		void toStream(BinaryStream &stream) const;

		double minx() const;
		double maxx() const;
		double miny() const;
		double maxy() const;

		void enlarge(int pixels);

		uint32_t xres, yres;

};

#endif
