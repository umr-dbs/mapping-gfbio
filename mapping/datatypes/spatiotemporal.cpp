#include "datatypes/spatiotemporal.h"
#include "raster/exceptions.h"
#include "operators/operator.h"
#include "util/binarystream.h"
#include "util/debug.h"

#include <math.h>
#include <limits>
#include <sstream>


SpatioTemporalReference::SpatioTemporalReference(epsg_t epsg, timetype_t timetype) : epsg(epsg), timetype(timetype) {
	x1 = -std::numeric_limits<double>::infinity();
	y1 = -std::numeric_limits<double>::infinity();
	x2 = std::numeric_limits<double>::infinity();
	y2 = std::numeric_limits<double>::infinity();

	t1 = -std::numeric_limits<double>::infinity();
	t2 = std::numeric_limits<double>::infinity();

	validate();
};


SpatioTemporalReference::SpatioTemporalReference(epsg_t epsg, double x1, double y1, double x2, double y2, timetype_t timetype, double t1, double t2)
	: epsg(epsg), timetype(timetype), x1(x1), y1(y1), x2(x2), y2(y2), t1(t1), t2(t2) {
	validate();
};

SpatioTemporalReference::SpatioTemporalReference(epsg_t epsg, double x1, double y1, double x2, double y2, bool &flipx, bool &flipy, timetype_t time, double t1, double t2)
	: epsg(epsg), timetype(timetype), x1(x1), y1(y1), x2(x2), y2(y2), t1(t1), t2(t2) {
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

SpatioTemporalReference::SpatioTemporalReference(const QueryRectangle &rect) {
	x1 = std::min(rect.x1, rect.x2);
	x2 = std::max(rect.x1, rect.x2);

	y1 = std::min(rect.y1, rect.y2);
	y2 = std::max(rect.y1, rect.y2);

	epsg = rect.epsg;

	t1 = rect.timestamp;
	t2 = t1;
	timetype = TIMETYPE_UNIX;

	validate();
};

SpatioTemporalReference::SpatioTemporalReference(BinaryStream &stream) {
	uint32_t uint;
	stream.read(&uint);
	epsg = (epsg_t) uint;
	stream.read(&uint);
	timetype = (timetype_t) uint;

	stream.read(&x1);
	stream.read(&y1);
	stream.read(&x2);
	stream.read(&y2);
	stream.read(&t1);
	stream.read(&t2);

	validate();
}

void SpatioTemporalReference::toStream(BinaryStream &stream) const {
	stream.write((uint32_t) epsg);
	stream.write((uint32_t) timetype);
	stream.write(x1);
	stream.write(y1);
	stream.write(x2);
	stream.write(y2);
	stream.write(t1);
	stream.write(t2);
}


void SpatioTemporalReference::validate() const {
	if (x1 > x2 || y1 > y2 || t1 > t2) {
		std::stringstream msg;
		msg << "SpatioTemporalReference invalid, requires x1:" << x1 << " <= x2:" << x2 << ", y1:" << y1 << " <= y2:" << y2 << " and t1:" << t1 << " <= t2:" << t2;
		throw ArgumentException(msg.str());
	}
}


void SpatioTemporalResult::replaceSTRef(const SpatioTemporalReference &newstref) {
	const_cast<SpatioTemporalReference&>(this->stref) = newstref;
}

epsg_t epsgCodeFromSrsString(const std::string &srsString, epsg_t def) {
	if (srsString == "")
		return def;
	if (srsString.compare(0,5,"EPSG:") == 0)
		return (epsg_t) std::stoi(srsString.substr(5, std::string::npos));
	throw ArgumentException("Unknown CRS specified");
}


