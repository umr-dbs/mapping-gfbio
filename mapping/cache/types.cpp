/*
 * types.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#include "cache/types.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"
#include "operators/operator.h"
#include "util/log.h"

CacheCube::CacheCube(const SpatioTemporalReference& stref) :
		SpatioTemporalReference(stref) {
	if (stref.timetype != TIMETYPE_UNIX)
		throw ArgumentException("CacheCube only accepts unix-timestamps");
}

CacheCube::CacheCube(epsg_t epsg, double x1, double x2, double y1, double y2, double t1, double t2) :
		SpatioTemporalReference(epsg, x1, x2, y1, y2, TIMETYPE_UNIX, t1, t2) {
}

CacheCube::CacheCube(BinaryStream& stream) :
		SpatioTemporalReference(stream) {
}

void CacheCube::toStream(BinaryStream& stream) const {
	SpatioTemporalReference::toStream(stream);
}

bool CacheCube::matches(const QueryRectangle& spec) const {
	return (spec.epsg == epsg && spec.x1 >= x1 && spec.x2 <= x2 && spec.y1 >= y1 && spec.y2 <= y2
			&& spec.timestamp >= t1 && spec.timestamp <= t2);
}

RasterCacheCube::RasterCacheCube(epsg_t epsg, double x1, double x2, double y1, double y2, double t1,
		double t2, double x_res_from, double x_res_to, double y_res_from, double y_res_to) :
		CacheCube(epsg, x1, x2, y1, y2, t1, t2), x_res_from(x_res_from), x_res_to(x_res_to), y_res_from(
				y_res_from), y_res_to(y_res_to) {
}

RasterCacheCube::RasterCacheCube(const GenericRaster& result) :
		CacheCube(result.stref) {

	double ohspan = result.stref.x2 - result.stref.x1;
	double ovspan = result.stref.y2 - result.stref.y1;

	// Enlarge result by degrees of half a pixel in each direction
	double h_spacing = ohspan / result.width / 100.0;
	double v_spacing = ovspan / result.height / 100.0;

	x1 = result.stref.x1 - h_spacing;
	x2 = result.stref.x2 + h_spacing;
	y1 = result.stref.y1 - v_spacing;
	y2 = result.stref.y2 + v_spacing;

	// Calc resolution bounds: (res/2, res]
	double h_pixel_per_deg = result.width / ohspan;
	double v_pixel_per_deg = result.width / ohspan;

	x_res_from = h_pixel_per_deg * 0.75;
	x_res_to = h_pixel_per_deg * 1.5;

	y_res_from = v_pixel_per_deg * 0.75;
	y_res_to = v_pixel_per_deg * 1.5;
}

RasterCacheCube::RasterCacheCube(BinaryStream& stream) :
		CacheCube(stream) {
	stream.read(&x_res_from);
	stream.read(&x_res_to);
	stream.read(&y_res_from);
	stream.read(&y_res_to);
}

void RasterCacheCube::toStream(BinaryStream& stream) const {
	CacheCube::toStream(stream);
	stream.write(x_res_from);
	stream.write(x_res_to);
	stream.write(y_res_from);
	stream.write(y_res_to);
}

bool RasterCacheCube::matches(const QueryRectangle& query) const {
	double q_x_res = (double) query.xres / (query.x2 - query.x1);
	double q_y_res = (double) query.xres / (query.x2 - query.x1);

	Log::trace("Matching resultion. Mine: [%f,%f]x[%f,%f], Query: %fx%f", x_res_from, x_res_to, y_res_from, y_res_to, q_x_res, q_y_res);

	return CacheCube::matches(query) && x_res_from < q_x_res && x_res_to >= q_x_res && y_res_from < q_y_res
			&& y_res_to >= q_y_res;
}

RasterRef::RasterRef(uint32_t node_id, uint64_t cache_id, const RasterCacheCube& cube) :
	node_id(node_id), cache_id(cache_id), cube(cube) {
}


