#ifndef OPERATORS_QUERYRECTANGLE_H
#define OPERATORS_QUERYRECTANGLE_H

#include "datatypes/spatiotemporal.h"

class BinaryStream;

class QueryResolution {
	public:
		enum class Type : uint16_t {
			NONE,
			PIXELS
		};

		QueryResolution() = delete;
		QueryResolution(Type restype, uint32_t xres, uint32_t yres) : restype(restype), xres(xres), yres(yres) {
		}
		QueryResolution(BinaryStream &stream);
		void toStream(BinaryStream &stream) const;


		static QueryResolution pixels(uint32_t xres, uint32_t yres) {
			return QueryResolution(Type::PIXELS, xres, yres);
		}
		static QueryResolution none() {
			return QueryResolution(Type::NONE, 0, 0);
		}


		Type restype;
		uint32_t xres;
		uint32_t yres;
};

class QueryRectangle : public SpatialReference, public TemporalReference, public QueryResolution {
	public:
		QueryRectangle();
		QueryRectangle(const SpatialReference &s, const TemporalReference &t, const QueryResolution &r) : SpatialReference(s), TemporalReference(t), QueryResolution(r) {}
		QueryRectangle(const GridSpatioTemporalResult &grid);
		QueryRectangle(BinaryStream &stream);

		void toStream(BinaryStream &stream) const;

		double minx() const;
		double maxx() const;
		double miny() const;
		double maxy() const;

		void enlarge(int pixels);
};

#endif