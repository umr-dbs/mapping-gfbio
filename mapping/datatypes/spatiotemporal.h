#ifndef DATATYPES_SPATIOTEMPORALREFERENCE_H
#define DATATYPES_SPATIOTEMPORALREFERENCE_H

#include <cstdint>
#include <cmath>
#include <string>

/*
 * Coordinate systems for space, mapping an x/y coordinate to a place on earth.
 */
enum epsg_t : uint16_t {
	EPSG_UNKNOWN = 0,
	EPSG_UNREFERENCED = 1,
	EPSG_GEOSMSG = 0x9E05,    // 40453  // GEOS -> this is only valid for origin 0� 0� and satellite_height 35785831 (Proj4) [
	EPSG_WEBMERCATOR = 3857, // 3785 is deprecated
	EPSG_LATLON = 4326 // http://spatialreference.org/ref/epsg/wgs-84/
};

/**
 * This function parses the ID of an EPSG-spatial reference system
 * @param srsString The string containing the SRS as 'AUTHORITY:ID'
 * @param def The default value to return if srsString is empty -> ""
 */
epsg_t epsgCodeFromSrsString(const std::string &srsString, epsg_t def = EPSG_WEBMERCATOR);

/*
 * Coordinate systems for time, mapping a t value to a time.
 */
enum timetype_t : uint16_t {
	TIMETYPE_UNKNOWN = 0,
	TIMETYPE_UNREFERENCED = 1,
	TIMETYPE_UNIX = 2
};

class QueryRectangle;
class BinaryStream;

/**
 * This class models a rectangle in space and an interval in time as a 3-dimensional cube.
 *
 * It is used both in the QueryRectangle (operators/operator.h) to specify a time and region of interest as well
 * as in all supported result types to specify the time and region the results apply to.
 *
 * This class expects:
 *  x1 <= x2 (usually east->west)
 *  y1 <= y2 (usually south->north - sorry!)
 *  t1 <= t2
 *  at all times. Failure will be met with an exception.
 *
 * The default constructor will create an invalid reference.
 */
class SpatioTemporalReference {
	public:
		/*
		 * No default constructor.
		 */
		SpatioTemporalReference() = delete;
		/**
		 * Construct a reference that spans the known universe, both in time and space.
		 * The actual endpoints are taken from the epsg_t or timetype_t when known;
		 * if in doubt they're set to +- Infinity.
		 */
		SpatioTemporalReference(epsg_t epsg, timetype_t timetype);
		/*
		 * Constructs a reference with all values
		 */
		SpatioTemporalReference(epsg_t epsg, double x1, double y1, double x2, double y2, timetype_t time, double t1, double t2);
		/*
		 * Constructs a reference with all values, but flips the endpoints if required
		 */
		SpatioTemporalReference(epsg_t epsg, double x1, double y1, double x2, double y2, bool &flipx, bool &flipy, timetype_t time, double t1, double t2);
		/*
		 * Constructs a reference from a QueryRectangle
		 */
		SpatioTemporalReference(const QueryRectangle &rect);
		/*
		 * Read a SpatioTemporalReference from a stream
		 */
		SpatioTemporalReference(BinaryStream &stream);
		/*
		 * Write to a binary stream
		 */
		void toStream(BinaryStream &stream) const;
		/*
		 * Validate if all invariants are met
		 */
		void validate() const;

		/*
		 * Named constructor for returning a reference that returns a valid reference which does not reference any
		 * point in space or time.
		 * This shall be used to instantiate rasters etc without an actual geo-reference.
		 */
		static SpatioTemporalReference unreferenced() {
			return SpatioTemporalReference(EPSG_UNREFERENCED, 0.0, 0.0, 1.0, 1.0, TIMETYPE_UNREFERENCED, 0.0, 1.0);
		}

		epsg_t epsg;
		timetype_t timetype;
		double x1, y1, x2, y2;
		double t1, t2;
};

/**
 * This is a base class for all results. SpatioTemporalReference is not inherited directly,
 * but added as a member, to properly model the HAS-A-relationship.
 */
class SpatioTemporalResult {
	public:
		SpatioTemporalResult() = delete;
		SpatioTemporalResult(const SpatioTemporalReference &stref) : stref(stref) {};
		void replaceSTRef(const SpatioTemporalReference &stref);

		const SpatioTemporalReference stref;

};

/*
 * This is a base class for all results on a grid, like Rasters.
 */
class GridSpatioTemporalResult : public SpatioTemporalResult {
	public:
		GridSpatioTemporalResult() = delete;
		GridSpatioTemporalResult(const SpatioTemporalReference &stref, uint32_t width, uint32_t height)
			: SpatioTemporalResult(stref), width(width), height(height), pixel_scale_x((stref.x2 - stref.x1) / width), pixel_scale_y((stref.y2 - stref.y1) / height) {
		};

		uint64_t getPixelCount() const { return (uint64_t) width * height; }

		double PixelToWorldX(int px) const { return stref.x1 + (px+0.5) * pixel_scale_x; }
		double PixelToWorldY(int py) const { return stref.y1 + (py+0.5) * pixel_scale_y; }

		int64_t WorldToPixelX(double wx) const { return floor((wx - stref.x1) / pixel_scale_x); }
		int64_t WorldToPixelY(double wy) const { return floor((wy - stref.y1) / pixel_scale_y); }

		const uint32_t width, height;
		const double pixel_scale_x, pixel_scale_y;
};



#endif
