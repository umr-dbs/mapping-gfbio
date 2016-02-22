#ifndef DATATYPES_SPATIOTEMPORALREFERENCE_H
#define DATATYPES_SPATIOTEMPORALREFERENCE_H

#include "datatypes/attributes.h"

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

class SpatialReference {
	public:
		/*
		 * No default constructor.
		 */
		SpatialReference() = delete;
		/**
		 * Construct a reference that spans the known universe.
		 * The actual endpoints are taken from the epsg_t t when known;
		 * if in doubt they're set to +- Infinity.
		 */
		SpatialReference(epsg_t epsg);
		/*
		 * Constructs a reference with all values
		 */
		SpatialReference(epsg_t epsg, double x1, double y1, double x2, double y2);
		/*
		 * Constructs a reference with all values, but flips the endpoints if required
		 */
		SpatialReference(epsg_t epsg, double x1, double y1, double x2, double y2, bool &flipx, bool &flipy);
		/*
		 * Read a SpatialReference from a stream
		 */
		SpatialReference(BinaryStream &stream);
		/*
		 * Write to a binary stream
		 */
		void toStream(BinaryStream &stream) const;
		/*
		 * Validate if all invariants are met
		 */
		void validate() const;

		/*
		 * Returns whether the other SpatialReference is contained (smaller or equal) within this.
		 * Throws an exception if the crs don't match.
		 */
		bool contains(const SpatialReference &other) const;

		/*
		 * Returns whether the point (x, y) is contained within this rectangle.
		 */
		bool contains(double x, double y) const {
			return x >= x1 && x <= x2 && y >= y1 && y <= y2;
		}

		/**
		 * the size of this object in memory (in bytes)
		 * @return the size of this object in bytes
		 */
		size_t get_byte_size() const;

		/*
		 * Named constructor for returning a reference that returns a valid reference which does not reference any
		 * point in space.
		 * This shall be used to instantiate rasters etc without an actual geo-reference.
		 */
		static SpatialReference unreferenced() {
			return SpatialReference(EPSG_UNREFERENCED, 0.0, 0.0, 1.0, 1.0);
		}
		/*
		 * Named constructor for returning a reference that spans the whole earth in the given CRS
		 */
		static SpatialReference extent(epsg_t epsg);

		// TODO: split into crs_authority_t / crs_code_t
		epsg_t epsg;
		double x1, y1, x2, y2;
};

/**
 * This class represents a closed-open time interval [t1, t2)
 */
class TimeInterval {
public:
	TimeInterval(double t1, double t2);

	TimeInterval(): t1(0), t2(0) {};

	/*
	 * Read a TimeInterval from a stream
	 */
	TimeInterval(BinaryStream &stream);

	/*
	 * Write to a binary stream
	 */
	void toStream(BinaryStream &stream) const;

	/*
	 * Validate if invariants are met
	 */
	void validate() const;

	/**
	 * Returns whether the other TimeInterval is contained (smaller or equal) within this.
	 * @param other the other TimeInterval
	 */
	bool contains(const TimeInterval &other) const;

	/**
	 * Returns whether the other TimeInterval intersects this
	 * @param other the other TimeInterval
	 * @return whether the intervals intersect
	 */
	bool intersects(const TimeInterval &other) const;

	/**
	 * Returns whether the given interval intersects this.
	 * @param t_start beginning of the interval
	 * @param t_end end of the interval
	 * @return whether the intervals intersect
	 */
	bool intersects(double t_start, double t_end) const;

	/**
	 * Sets this interval to the intersection of the two intervals.
	 * @param other the time interval for intersection
	 */
	void intersect(const TimeInterval &other);

	/**
	 * compute the intersection of this interval with another
	 * @param other the other time interval
	 * @return a new interval representing the intersection of the two intervals
	 */
	TimeInterval intersection(const TimeInterval &other);

	/**
	 * Sets this interval to the union of the two intervals if they intersect
	 * Throws an ArgumentException if the intervals don't intersect
	 * @param other the other time interval
	 */
	void union_with(TimeInterval &other);

	/**
	 * the size of this object in memory (in bytes)
	 * @return the size of this object in bytes
	 */
	size_t get_byte_size() const;

	double t1, t2;
};

class TemporalReference : public TimeInterval {
	public:
		/*
		 * No default constructor.
		 */
		TemporalReference() = delete;
		/**
		 * Construct a reference that spans the known universe in time.
		 * The actual endpoints are taken from the timetype_t when known;
		 * if in doubt they're set to +- Infinity.
		 */
		TemporalReference(timetype_t timetype);
		/*
		 * Constructs a reference with all values
		 */
		TemporalReference(timetype_t time, double t1, double t2);
		/*
		 * Read a TemporalReference from a stream
		 */
		TemporalReference(BinaryStream &stream);
		/*
		 * Write to a binary stream
		 */
		void toStream(BinaryStream &stream) const;
		/*
		 * Validate if all invariants are met
		 */
		void validate() const;

		/*
		 * Returns the smallest valid timestamp based on the timetype
		 */
		double beginning_of_time() const;
		/*
		 * Returns the highest valid timestamp based on the timetype
		 */
		double end_of_time() const;

		/*
		 * Returns whether the other TemporalReference is contained (smaller or equal) within this.
		 * Throws an exception if the timetypes don't match.
		 */
		bool contains(const TemporalReference &other) const;

		/*
		 * Returns whether the other TemporalReference intersects this
		 * Throws an exception if the timetypes don't match.
		 * @param other the other TemporalReference
		 * @return whether the intervals intersect
		 */
		bool intersects(const TemporalReference &other) const;
		/*
		 * Returns whether the given interval intersects this.
		 * The interval is assumed to have the same timetype as this
		 * @param t_start beginning of the interval
		 * @param t_end end of the interval
		 * @return whether the intervals intersect
		 */
		bool intersects(double t_start, double t_end) const;

		/*
		 * Sets this reference to the intersection of the two references.
		 */
		void intersect(const TemporalReference &other);

		/**
		 * serializes given time as ISO String with respect to timetype
		 */
		std::string toIsoString(double time) const ;

		/**
		 * the size of this object in memory (in bytes)
		 * @return the size of this object in bytes
		 */
		size_t get_byte_size() const;

		/*
		 * Named constructor for returning a reference that returns a valid reference which does not reference any
		 * point in time.
		 * This shall be used to instantiate rasters etc without an actual geo-reference.
		 */
		static TemporalReference unreferenced() {
			return TemporalReference(TIMETYPE_UNREFERENCED, 0.0, 1.0);
		}
		timetype_t timetype;

		static const std::string ISO_BEGIN_OF_TIME;
		static const std::string ISO_END_OF_TIME;
};

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
class SpatioTemporalReference : public SpatialReference, public TemporalReference {
	public:
		/*
		 * No default constructor.
		 */
		SpatioTemporalReference() = delete;
		/*
		 * Constructs a reference from a SpatialReference and a TemporalReference
		 */
		SpatioTemporalReference(const SpatialReference &sref, const TemporalReference &tref)
			: SpatialReference(sref), TemporalReference(tref) {};
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

		/**
		 * the size of this object in memory (in bytes)
		 * @return the size of this object in bytes
		 */
		size_t get_byte_size() const;

		/*
		 * Named constructor for returning a reference that returns a valid reference which does not reference any
		 * point in space or time.
		 * This shall be used to instantiate rasters etc without an actual geo-reference.
		 */
		static SpatioTemporalReference unreferenced() {
			return SpatioTemporalReference(SpatialReference::unreferenced(), TemporalReference::unreferenced());
		}
};

/**
 * This is a base class for all results. SpatioTemporalReference is not inherited directly,
 * but added as a member, to properly model the HAS-A-relationship.
 */
class SpatioTemporalResult {
	public:
		SpatioTemporalResult() = delete;
		SpatioTemporalResult(const SpatioTemporalReference &stref) : stref(stref) {};
		// Globally disable copying. SpatioTemporalResults are usually large data structures and we want to avoid accidental copies.
		SpatioTemporalResult(const SpatioTemporalResult &) = delete;
		SpatioTemporalResult& operator=(const SpatioTemporalResult &) = delete;
		SpatioTemporalResult(SpatioTemporalResult &&) = default;

		virtual ~SpatioTemporalResult() = default;

		SpatioTemporalResult& operator=(SpatioTemporalResult &&other) {
			replaceSTRef(other.stref);
			global_attributes = std::move(other.global_attributes);
			return *this;
		}

		void replaceSTRef(const SpatioTemporalReference &stref);

		virtual size_t get_byte_size() const;

		const SpatioTemporalReference stref;
		AttributeMaps global_attributes;

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

		virtual ~GridSpatioTemporalResult() = default;

		uint64_t getPixelCount() const { return (uint64_t) width * height; }

		double PixelToWorldX(int px) const { return stref.x1 + (px+0.5) * pixel_scale_x; }
		double PixelToWorldY(int py) const { return stref.y1 + (py+0.5) * pixel_scale_y; }

		int64_t WorldToPixelX(double wx) const { return floor((wx - stref.x1) / pixel_scale_x); }
		int64_t WorldToPixelY(double wy) const { return floor((wy - stref.y1) / pixel_scale_y); }

		virtual size_t get_byte_size() const;

		const uint32_t width, height;
		const double pixel_scale_x, pixel_scale_y;
};



#endif
