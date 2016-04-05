#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"
#include "operators/operator.h"
#include "util/binarystream.h"
#include "util/debug.h"
#include "cache/common.h"
#include "util/timeparser.h"

#include <math.h>
#include <limits>
#include <sstream>
#include <ctime>
#include <iomanip>


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

SpatialReference::SpatialReference(BinaryReadBuffer &buffer) {
	uint32_t uint;
	buffer.read(&uint);
	epsg = (epsg_t) uint;

	buffer.read(&x1);
	buffer.read(&y1);
	buffer.read(&x2);
	buffer.read(&y2);

	validate();
}

void SpatialReference::serialize(BinaryWriteBuffer &buffer, bool) const {
	buffer
		<< (uint32_t) epsg
		<< x1 << y1 << x2 << y2
	;
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

TimeInterval::TimeInterval(BinaryReadBuffer &buffer) {
	buffer.read(&t1);
	buffer.read(&t2);

	validate();
}

void TimeInterval::serialize(BinaryWriteBuffer &buffer, bool) const {
	buffer << t1 << t2;
}

void TimeInterval::validate() const {
	if (t1 >= t2)
		throw ArgumentException(concat("TimeInterval invalid, requires t1:", t1, " < t2:", t2, "\n", CacheCommon::get_stacktrace()));
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
	if (t1 >= t2)
		throw ArgumentException("intersect(): both TimeIntervals do not intersect");
}

TimeInterval TimeInterval::intersection(const TimeInterval &other) {
	double intersectiont1 = std::max(t1, other.t1);
	double intersectiont2 = std::min(t2, other.t2);
	if (intersectiont1 >= intersectiont2)
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


double calculateBOT() {
	auto timeParser = TimeParser::create(TimeParser::Format::ISO);
	return timeParser->parse("0001-01-01T00:00:00");
}

double calculateEOT() {
	auto timeParser = TimeParser::create(TimeParser::Format::ISO);
	return timeParser->parse("9999-12-31T23:59:59");
}

const double TemporalReference::begin_of_time_value = calculateBOT();
const double TemporalReference::end_of_time_value = calculateEOT();




TemporalReference::TemporalReference(timetype_t timetype) : TimeInterval(), timetype(timetype) {
	t1 = beginning_of_time();
	t2 = end_of_time();

	validate();
}

TemporalReference::TemporalReference(timetype_t timetype, double t1) : TimeInterval(), timetype(timetype) {
	this->t1 = t1;
	this->t2 = t1+epsilon();
	if (this->t1 >= this->t2)
		throw MustNotHappenException(concat("TemporalReference::epsilon() too small for this magnitude, ", this->t1, " == ", this->t2));

	validate();
}

TemporalReference::TemporalReference(timetype_t timetype, double t1, double t2)
	: TimeInterval(t1, t2), timetype(timetype) {
	validate();
}


TemporalReference::TemporalReference(BinaryReadBuffer &buffer) : TimeInterval(buffer) {
	timetype = (timetype_t) buffer.read<uint32_t>();;

	validate();
}

void TemporalReference::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	TimeInterval::serialize(buffer, is_persistent_memory);
	buffer << (uint32_t) timetype;
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
		//ISO 8601: 0001-01-01T00:00:00
		return begin_of_time_value;
	}
	// The default for other timetypes is -infinity
	return -std::numeric_limits<double>::infinity();
}

double TemporalReference::end_of_time() const {
	if (timetype == TIMETYPE_UNIX) {
		//ISO 8601: 9999-12-31T23:59:59
		return end_of_time_value;
	}
	// The default for other timetypes is infinity
	return std::numeric_limits<double>::infinity();
}

double TemporalReference::epsilon() const {
	if (timetype == TIMETYPE_UNIX) {
		return 1.0/1000.0; // 1 millisecond should be small enough.
	}
	throw ArgumentException(concat("TemporalReference::epsilon() on unknown timetype ", (int) timetype, "\n"));
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

	if(timetype == TIMETYPE_UNIX){
		long t = time;
		std::tm *tm = std::gmtime(&t);

		if(tm == NULL)
			throw ArgumentException("Could not convert time to IsoString");

		int year = 1900 + tm->tm_year;
		int month = tm->tm_mon + 1;
		int day = tm->tm_mday;
		int hour = tm->tm_hour;
		int minute = tm->tm_min;
		int second = tm->tm_sec;

		double frac = time - t;

		result.fill('0');

		result << std::setw(4) << year << "-" << std::setw(2) << month << "-" <<std::setw(2) << day << "T" <<
				std::setw(2) << hour << ":" <<std::setw(2) << minute << ":" <<std::setw(2) << second;

		if(frac > 0.0) {
			//remove leading zero
			std::stringstream ss;
			ss << frac;
			std::string frac_string = ss.str();

			frac_string.erase(0, frac_string.find_first_not_of('0'));

			result << frac_string;
		}

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
SpatioTemporalReference::SpatioTemporalReference(BinaryReadBuffer &buffer) : SpatialReference(buffer), TemporalReference(buffer) {
}

void SpatioTemporalReference::serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const {
	SpatialReference::serialize(buffer, is_persistent_memory);
	TemporalReference::serialize(buffer, is_persistent_memory);
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
