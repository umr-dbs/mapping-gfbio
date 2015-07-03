/*
 * types.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#include "cache/priv/connection.h"
#include "cache/priv/types.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"
#include "operators/operator.h"
#include "util/log.h"

#include <cstdio>

////////////////////////////////////////////////////////
//
// Cache-Key
//
////////////////////////////////////////////////////////


STCacheKey::STCacheKey(const std::string& semantic_id, uint64_t entry_id) :
		semantic_id(semantic_id), entry_id(entry_id) {
}

STCacheKey::STCacheKey(BinaryStream& stream) {
	stream.read(&semantic_id);
	stream.read(&entry_id);
}

void STCacheKey::toStream(BinaryStream& stream) const {
	stream.write(semantic_id);
	stream.write(entry_id);
}

std::string STCacheKey::to_string() const {
	std::ostringstream ss;
	ss << "STCacheKey: " << semantic_id << ":" << entry_id;
	return ss.str();
}

////////////////////////////////////////////////////////
//
// Query-Info
//
////////////////////////////////////////////////////////

STQueryInfo::STQueryInfo(double coverage, double x1, double x2, double y1, double y2, uint64_t cache_id) :
	coverage(coverage), x1(x1), x2(x2), y1(y1), y2(y2), cache_id(cache_id) {
}

bool STQueryInfo::operator <(const STQueryInfo& b) const {
	return get_score() < b.get_score();
}

std::string STQueryInfo::to_string() const {
	std::ostringstream ss;
	ss << "STQueryInfo: [" << x1 << "," << x2 << "]x[" << y1 << "," << y2 << "], coverage: " << coverage << ", cache_id: " << cache_id;
	return ss.str();
}

double STQueryInfo::get_score() const {
	// Scoring over coverage and area
	return coverage / ((x2-x1)*(y2-y1));
}

////////////////////////////////////////////////////////
//
// Raster-Reference
//
////////////////////////////////////////////////////////

STRasterRef::STRasterRef(uint32_t node_id, uint64_t cache_id, const STRasterEntryBounds& bounds) :
	node_id(node_id), cache_id(cache_id), bounds(bounds) {
}

STRasterRefKeyed::STRasterRefKeyed(uint32_t node_id, const std::string& semantic_id, uint64_t cache_id,
	const STRasterEntryBounds& bounds) : STRasterRef(node_id,cache_id,bounds), semantic_id(semantic_id) {
}

STRasterRefKeyed::STRasterRefKeyed(uint32_t node_id, const STCacheKey& key,
	const STRasterEntryBounds& bounds) : STRasterRef(node_id,key.entry_id,bounds), semantic_id(key.semantic_id) {
}

////////////////////////////////////////////////////////
//
// EntryBounds
//
////////////////////////////////////////////////////////

STEntryBounds::STEntryBounds(const SpatioTemporalReference& stref) :
		SpatioTemporalReference(stref) {
	if (stref.timetype != TIMETYPE_UNIX)
		throw ArgumentException("CacheCube only accepts unix-timestamps");
}

STEntryBounds::STEntryBounds(epsg_t epsg, double x1, double x2, double y1, double y2, double t1, double t2) :
		SpatioTemporalReference(epsg, x1, x2, y1, y2, TIMETYPE_UNIX, t1, t2) {
}

STEntryBounds::STEntryBounds(BinaryStream& stream) :
		SpatioTemporalReference(stream) {
}

void STEntryBounds::toStream(BinaryStream& stream) const {
	SpatioTemporalReference::toStream(stream);
}

bool STEntryBounds::matches(const QueryRectangle& spec) const {
	return (spec.epsg == epsg && spec.x1 >= x1 && spec.x2 <= x2 && spec.y1 >= y1 && spec.y2 <= y2
			&& spec.timestamp >= t1 && spec.timestamp <= t2);
}

double STEntryBounds::get_coverage( const QueryRectangle &query ) const {
	if ( x1 > query.x2 || x2 < query.x1 ||
		 y1 > query.y2 || y2 < query.y1 )
		return 0.0;

	double ix1 = std::max(x1,query.x1),
		   ix2 = std::min(x2,query.x2),
		   iy1 = std::max(y1,query.y1),
		   iy2 = std::min(y2,query.y2);

	double iarea = std::abs((ix2-ix1) * (iy2-iy1));
	double qarea = std::abs((query.x2-query.x1) * (query.y2-query.y1));

	return iarea / qarea;
}

std::string STEntryBounds::to_string() const {
	std::ostringstream ss;
	ss << "STEntryBounds: ";
	ss << "x:[" << x1 << "," << x2 << "], ";
	ss << "y:[" << y1 << "," << y2 << "], ";
	ss << "t:[" << t1 << "," << t2 << "]";
	return ss.str();
}


////////////////////////////////////////////////////////
//
// RasterEntryBounds
//
////////////////////////////////////////////////////////


STRasterEntryBounds::STRasterEntryBounds(epsg_t epsg, double x1, double x2, double y1, double y2, double t1,
		double t2, double x_res_from, double x_res_to, double y_res_from, double y_res_to) :
		STEntryBounds(epsg, x1, x2, y1, y2, t1, t2), x_res_from(x_res_from), x_res_to(x_res_to), y_res_from(
				y_res_from), y_res_to(y_res_to) {
}

STRasterEntryBounds::STRasterEntryBounds(const GenericRaster& result) :
		STEntryBounds(result.stref) {

	double ohspan = result.stref.x2 - result.stref.x1;
	double ovspan = result.stref.y2 - result.stref.y1;

	// Enlarge result by degrees of half a pixel in each direction
	double h_spacing = ohspan / result.width / 100.0;
	double v_spacing = ovspan / result.height / 100.0;

	x1 = result.stref.x1 - h_spacing;
	x2 = result.stref.x2 + h_spacing;
	y1 = result.stref.y1 - v_spacing;
	y2 = result.stref.y2 + v_spacing;

	// Calc resolution bounds: (res*.75, res*1.5]
	double h_pixel_per_deg = result.width / ohspan;
	double v_pixel_per_deg = result.width / ohspan;

	x_res_from = h_pixel_per_deg * 0.75;
	x_res_to = h_pixel_per_deg * 1.5;

	y_res_from = v_pixel_per_deg * 0.75;
	y_res_to = v_pixel_per_deg * 1.5;
}

STRasterEntryBounds::STRasterEntryBounds(BinaryStream& stream) :
		STEntryBounds(stream) {
	stream.read(&x_res_from);
	stream.read(&x_res_to);
	stream.read(&y_res_from);
	stream.read(&y_res_to);
}

void STRasterEntryBounds::toStream(BinaryStream& stream) const {
	STEntryBounds::toStream(stream);
	stream.write(x_res_from);
	stream.write(x_res_to);
	stream.write(y_res_from);
	stream.write(y_res_to);
}

bool STRasterEntryBounds::matches(const QueryRectangle& query) const {
	double q_x_res = (double) query.xres / (query.x2 - query.x1);
	double q_y_res = (double) query.yres / (query.y2 - query.y1);

	return STEntryBounds::matches(query) &&
			x_res_from <  q_x_res &&
			x_res_to   >= q_x_res &&
			y_res_from <  q_y_res &&
			y_res_to   >= q_y_res;
}


double STRasterEntryBounds::get_coverage( const QueryRectangle &query ) const {
	double q_x_res = (double) query.xres / (query.x2 - query.x1);
	double q_y_res = (double) query.yres / (query.y2 - query.y1);

	if ( x_res_from <  q_x_res &&
		 x_res_to   >= q_x_res &&
		 y_res_from <  q_y_res &&
		 y_res_to   >= q_y_res )
		return STEntryBounds::get_coverage(query);
	else
		return 0.0;
}

std::string STRasterEntryBounds::to_string() const {
	std::ostringstream ss;
	ss << "STRasterEntryBounds: ";
	ss << "x:[" << x1 << "," << x2 << "], ";
	ss << "y:[" << y1 << "," << y2 << "], ";
	ss << "t:[" << t1 << "," << t2 << "], ";
	ss << "x_res:[" << x_res_from << "," << x_res_to << "], ";
	ss << "y_res:[" << y_res_from << "," << y_res_to << "]";
	return ss.str();
}

Delivery::Delivery(uint64_t id, unsigned int count, std::unique_ptr<GenericRaster> &raster) :
	id(id), creation_time(time(0)), count(count), type(Type::RASTER), raster(std::move(raster)) {
}

void Delivery::send(DeliveryConnection& connection) {
	count--;
	switch ( type ) {
		case Type::RASTER: {
			connection.send_raster( *raster );
			break;
		}
		default: {
			Log::error("Currently only raster supported");
		}
	}
}

Delivery::Delivery(Delivery&& d) :
	id(d.id), creation_time(d.creation_time),
	count(d.count), type(d.type), raster(std::move(d.raster)) {
}
