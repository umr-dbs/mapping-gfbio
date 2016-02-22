#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"
#include "operators/operator.h"
#include "util/binarystream.h"
#include "util/debug.h"
#include "cache/common.h"

#include "boost/date_time/posix_time/posix_time.hpp"
#include <math.h>
#include <limits>
#include <sstream>


/**
 * SpatialReference
 */
SpatialReference::SpatialReference(epsg_t epsg) : epsg(epsg) {
	x1 = -std::numeric_limits<double>::infinity();
	y1 = -std::numeric_limits<double>::infinity();
	x2 = std::numeric_limits<double>::infinity();
	y2 = std::numeric_limits<double>::infinity();

	validate();
}

SpatialReference::SpatialReference(epsg_t epsg, double x1, double y1, double x2, double y2)
	: epsg(epsg), x1(x1), y1(y1), x2(x2), y2(y2) {
	validate();
}

SpatialReference::SpatialReference(epsg_t epsg, double x1, double y1, double x2, double y2, bool &flipx, bool &flipy)
	: epsg(epsg), x1(x1), y1(y1), x2(x2), y2(y2) {
	flipx = flipy = false;
	if (x1 > x2) {
		flipx = true;
		std::swap(this->x1, this->x2);
	}
	if (y1 > y2) {
		flipy = true;
		std::swap(this->y1, this->y2);
	}
	validate();
}

SpatialReference::SpatialReference(BinaryStream &stream) {
	uint32_t uint;
	stream.read(&uint);
	epsg = (epsg_t) uint;

	stream.read(&x1);
	stream.read(&y1);
	stream.read(&x2);
	stream.read(&y2);

	validate();
}

void SpatialReference::toStream(BinaryStream &stream) const {
	stream.write((uint32_t) epsg);
	stream.write(x1);
	stream.write(y1);
	stream.write(x2);
	stream.write(y2);
}

/*
 * Returns whether the other SpatialReference is contained (smaller or equal) within this.
 * Throws an exception if the crs don't match
 */
bool SpatialReference::contains(const SpatialReference &other) const {
	if (epsg != other.epsg)
		throw ArgumentException("SpatialReference::contains(): epsg don't match");
	if ( x1 <= other.x1 && x2 >= other.x2 && y1 <= other.y1 && y2 >= other.y2 )
		return true;

	//TODO: Talk about this
	auto ex = SpatialReference::extent(epsg);
	double xeps = (ex.x2-ex.x1)*std::numeric_limits<double>::epsilon();
	double yeps = (ex.y2-ex.y1)*std::numeric_limits<double>::epsilon();

	return ( (x1 - other.x1) < xeps  ) &&
		   ( (other.x2 - x2) < xeps ) &&
		   ( (y1 - other.y1) < yeps ) &&
		   ( (other.y2 - y2) < yeps );
}


void SpatialReference::validate() const {
	if (x1 > x2 || y1 > y2) {
		std::stringstream msg;
		msg << "SpatialReference invalid, requires x1:" << x1 << " <= x2:" << x2 << ", y1:" << y1 << " <= y2:" << y2;
		throw ArgumentException(msg.str());
	}
}

/**
 * TimeInterval
 */

TimeInterval::TimeInterval(double t1, double t2) : t1(t1), t2(t2) {
	validate();
}

TimeInterval::TimeInterval(BinaryStream &stream) {
	stream.read(&t1);
	stream.read(&t2);

	validate();
}

void TimeInterval::toStream(BinaryStream &stream) const {
	stream.write(t1);
	stream.write(t2);
}

void TimeInterval::validate() const {
	if (t1 > t2)
		throw ArgumentException(concat("TimeInterval invalid, requires t1:", t1, " <= t2:", t2, "\n", CacheCommon::get_stacktrace()));
}


bool TimeInterval::contains(const TimeInterval &other) const {
	return t1 <= other.t1 && t2 >= other.t2;
}

bool TimeInterval::intersects(const TimeInterval &other) const {
	return intersects(other.t1, other.t2);
}

bool TimeInterval::intersects(double t_start, double t_end) const {
	return t_start < this->t2 && t_end > this->t1;
}

void TimeInterval::intersect(const TimeInterval &other) {
	t1 = std::max(t1, other.t1);
	t2 = std::min(t2, other.t2);
	if (t1 > t2)
		throw ArgumentException("intersect(): both TimeIntervals do not intersect");
}

TimeInterval TimeInterval::intersection(const TimeInterval &other) {
	double intersectiont1 = std::max(t1, other.t1);
	double intersectiont2 = std::min(t2, other.t2);
	if (intersectiont1 > intersectiont2)
		throw ArgumentException("intersect(): both TimeIntervals do not intersect");
	return TimeInterval(intersectiont1, intersectiont2);
}

void TimeInterval::union_with(TimeInterval &other) {
	if(!intersects(other))
		throw ArgumentException("union_with() both TimeIntervals do not intersect");

	t1 = std::min(t1, other.t1);
	t2 = std::max(t2, other.t2);
}

size_t TimeInterval::get_byte_size() const {
	return sizeof(TimeInterval);
}

/**
 * TemporalReference
 */

size_t TemporalReference::get_byte_size() const {
	return sizeof(TemporalReference);
}

SpatialReference SpatialReference::extent(epsg_t epsg) {
	if (epsg == EPSG_WEBMERCATOR)
		return SpatialReference(EPSG_WEBMERCATOR, -20037508.34,-20037508.34,20037508.34,20037508.34);
	if (epsg == EPSG_LATLON)
		return SpatialReference(EPSG_LATLON, -180, -90, 180, 90);
	if (epsg == EPSG_GEOSMSG)
		return SpatialReference(EPSG_GEOSMSG, -5568748.276, -5568748.276, 5568748.276, 5568748.276);

	throw ArgumentException("Cannot return extent of an unknown CRS");
}

const std::string TemporalReference::ISO_BEGIN_OF_TIME = "-infinity";
const std::string TemporalReference::ISO_END_OF_TIME = "infinity";

TemporalReference::TemporalReference(timetype_t timetype) : TimeInterval(0, 0), timetype(timetype) {
	t1 = beginning_of_time();
	t2 = end_of_time();

	validate();
}


TemporalReference::TemporalReference(timetype_t timetype, double t1, double t2)
	: TimeInterval(t1, t2), timetype(timetype) {
	validate();
}


TemporalReference::TemporalReference(BinaryStream &stream) : TimeInterval(stream) {
	uint32_t uint;
	stream.read(&uint);
	timetype = (timetype_t) uint;

	validate();
}

void TemporalReference::toStream(BinaryStream &stream) const {
	TimeInterval::toStream(stream);
	stream.write((uint32_t) timetype);
}


void TemporalReference::validate() const {
	TimeInterval::validate();
	if (t1 < beginning_of_time())
		throw ArgumentException(concat("TemporalReference invalid, requires t1:", t1, " >= bot:", beginning_of_time()));
	if (t2 > end_of_time())
		throw ArgumentException(concat("TemporalReference invalid, requires t2:", t2, " <= eot:", end_of_time()));
}

double TemporalReference::beginning_of_time() const {
	if (timetype == TIMETYPE_UNIX) {
		// TODO: find a sensible value. Big Bang? Creation of earth?
		return -std::numeric_limits<double>::infinity();
	}
	// The default for other timetypes is -infinity
	return -std::numeric_limits<double>::infinity();
}

double TemporalReference::end_of_time() const {
	if (timetype == TIMETYPE_UNIX) {
		// TODO: find a sensible value. When the sun turns supernova in a couple billion years, that'd probably mark the end of earth-based geography.
		return std::numeric_limits<double>::infinity();
	}
	// The default for other timetypes is infinity
	return std::numeric_limits<double>::infinity();
}



bool TemporalReference::contains(const TemporalReference &other) const {
	if (timetype != other.timetype)
		throw ArgumentException("TemporalReference::contains(): timetypes don't match");

	return TimeInterval::contains(other);
}

bool TemporalReference::intersects(const TemporalReference &other) const {
	if (timetype != other.timetype)
		throw ArgumentException("TemporalReference::contains(): timetypes don't match");

	return TimeInterval::intersects(other);
}

bool TemporalReference::intersects(double t_start, double t_end) const {
	return TimeInterval::intersects(t_start, t_end);
}



void TemporalReference::intersect(const TemporalReference &other) {
	if (timetype != other.timetype)
		throw ArgumentException("Cannot intersect() TemporalReferences with different timetype");

	TimeInterval::intersect(other);
}

std::string TemporalReference::toIsoString(double time) const {
	std::ostringstream result;

	if(time == beginning_of_time())
		return TemporalReference::ISO_BEGIN_OF_TIME;
	else if(time == end_of_time())
		return ISO_END_OF_TIME;

	if(timetype == TIMETYPE_UNIX){
		result << boost::posix_time::to_iso_extended_string(boost::posix_time::from_time_t(time));
		//TODO: incorporate fractions of seconds
	} else {
		throw ConverterException("can only convert UNIX timestamps");
	}

	return result.str();
}


size_t SpatialReference::get_byte_size() const {
	return sizeof(SpatialReference);
}

/**
 * SpatioTemporalReference
 */
SpatioTemporalReference::SpatioTemporalReference(BinaryStream &stream) : SpatialReference(stream), TemporalReference(stream) {
}

void SpatioTemporalReference::toStream(BinaryStream &stream) const {
	SpatialReference::toStream(stream);
	TemporalReference::toStream(stream);
}

SpatioTemporalReference::SpatioTemporalReference(const QueryRectangle &rect) : SpatialReference(rect), TemporalReference(rect) {
}

void SpatioTemporalReference::validate() const {
	SpatialReference::validate();
	TemporalReference::validate();
}

size_t SpatioTemporalReference::get_byte_size() const {
	return sizeof(SpatioTemporalReference);
}


/**
 * SpatioTemporalResult
 */
void SpatioTemporalResult::replaceSTRef(const SpatioTemporalReference &newstref) {
	const_cast<SpatioTemporalReference&>(this->stref) = newstref;
}

size_t SpatioTemporalResult::get_byte_size() const {
	return stref.get_byte_size() + global_attributes.get_byte_size();
}

size_t GridSpatioTemporalResult::get_byte_size() const {
	return SpatioTemporalResult::get_byte_size() + 2 * sizeof(double) + 2 * sizeof(uint32_t);
}


/**
 * helper functions
 */
epsg_t epsgCodeFromSrsString(const std::string &srsString, epsg_t def) {
	if (srsString == "")
		return def;
	if (srsString.compare(0,5,"EPSG:") == 0)
		return (epsg_t) std::stoi(srsString.substr(5, std::string::npos));
	throw ArgumentException("Unknown CRS specified");
}
