/*
 * transfer.cpp
 *
 *  Created on: 11.06.2015
 *      Author: mika
 */

#include "cache/common.h"
#include "cache/priv/transfer.h"
#include "util/log.h"
#include <geos/io/WKBWriter.h>
#include <geos/io/WKBReader.h>

//
// Foreign reference
//

ForeignRef::ForeignRef(const std::string& host, uint32_t port) :
	host(host), port(port) {
}

ForeignRef::ForeignRef(BinaryStream& stream) {
	stream.read(&host);
	stream.read(&port);
}

void ForeignRef::toStream(BinaryStream& stream) const {
	stream.write(host);
	stream.write(port);
}

//
// Delivery response
//

DeliveryResponse::DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id) :
			ForeignRef(host,port), delivery_id(delivery_id) {
}

DeliveryResponse::DeliveryResponse(BinaryStream& stream) : ForeignRef(stream) {
	stream.read(&delivery_id);
}

void DeliveryResponse::toStream(BinaryStream& stream) const {
	ForeignRef::toStream(stream);
	stream.write(delivery_id);
}

std::string DeliveryResponse::to_string() const {
	std::ostringstream ss;
	ss << "DeliveryResponse[" << host << ":" << port << ", delivery_id: " << delivery_id << "]";
	return ss.str();
}

//
// Cache-entry reference
//


CacheRef::CacheRef(const std::string& host, uint32_t port, uint64_t entry_id) :
	ForeignRef(host,port), entry_id(entry_id) {
}

CacheRef::CacheRef(BinaryStream& stream) : ForeignRef(stream) {
	stream.read(&entry_id);
}

void CacheRef::toStream(BinaryStream& stream) const {
	ForeignRef::toStream(stream);
	stream.write(entry_id);
}

std::string CacheRef::to_string() const {
	std::ostringstream ss;
	ss << "CacheRef[" << host << ":" << port << ", entry_id: " << entry_id << "]";
	return ss.str();
}

//
// Requests
//


BaseRequest::BaseRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect) :
	type(type),
	semantic_id(sem_id),
	query(rect) {
}

BaseRequest::BaseRequest(BinaryStream& stream) : query(stream) {
	stream.read(&semantic_id);
	stream.read(&type);
}

void BaseRequest::toStream(BinaryStream& stream) const {
	query.toStream(stream);
	stream.write(semantic_id);
	stream.write(type);
}

std::string BaseRequest::to_string() const {
	std::ostringstream ss;
	ss << "BaseRequest:" << std::endl;
	ss << "  type: " << (int) type << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query);
	return ss.str();
}

//
// Delivery request
//

DeliveryRequest::DeliveryRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect, uint64_t entry_id) :
	BaseRequest(type,sem_id,rect), entry_id(entry_id) {
}

DeliveryRequest::DeliveryRequest(BinaryStream& stream) :
	BaseRequest(stream) {
	stream.read(&entry_id);
}

void DeliveryRequest::toStream(BinaryStream& stream) const {
	BaseRequest::toStream(stream);
	stream.write(entry_id);
}

std::string DeliveryRequest::to_string() const {
	std::ostringstream ss;
	ss << "DeliveryRequest:" << std::endl;
	ss << "  type: " << (int) type << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query) << std::endl;
	ss << "  entry_id: " << entry_id;
	return ss.str();
}

//
// Puzzle-Request
//

PuzzleRequest::PuzzleRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect,
	const std::vector<Cube<3>> &remainder, const std::vector<CacheRef>& parts) :
	BaseRequest(type,sem_id,rect),
	parts(parts),
	remainder( remainder ) {
}

PuzzleRequest::PuzzleRequest(BinaryStream& stream) :
	BaseRequest(stream) {

	uint64_t v_size;
	stream.read(&v_size);
	remainder.reserve(v_size);
	for ( uint64_t i = 0; i < v_size; i++ ) {
		remainder.push_back( Cube<3>(stream) );
	}


	stream.read(&v_size);
	parts.reserve(v_size);
	for ( uint64_t i = 0; i < v_size; i++ ) {
		parts.push_back( CacheRef(stream) );
	}
}

void PuzzleRequest::toStream(BinaryStream& stream) const {
	BaseRequest::toStream(stream);

	uint64_t v_size = remainder.size();
	stream.write(v_size);
	for ( auto &rem : remainder )
		rem.toStream(stream);

	v_size = parts.size();
	stream.write(v_size);
	for ( auto &cr : parts )
		cr.toStream(stream);
}

std::string PuzzleRequest::to_string() const {
	std::ostringstream ss;
	ss << "PuzzleRequest:" << std::endl;
	ss << "  type: " << (int) type << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query) << std::endl;
	ss << "  #remainder: " << remainder.size() << std::endl;
	ss << "  parts: [";

	for ( std::vector<CacheRef>::size_type i = 0; i < parts.size(); i++ ) {
		if ( i > 0 )
			ss << ", ";
		ss << parts[i].to_string();
	}
	ss << "]";

	return ss.str();
}

//
//
// WORK HERE
//std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries(const GridSpatioTemporalResult &ref) const {
//	double x1 = DoubleInfinity, x2 = DoubleNegInfinity, y1 = DoubleInfinity, y2 = DoubleNegInfinity;
//
//	std::vector<QueryRectangle> result;
//
//	for ( auto &rem : remainder ) {
//
//	//	Log::info("Remainder before snap: [%f,%f]x[%f,%f]",x1,x2,y1,y2);
//
//		double x1 = rem.get_dimension(0).a;
//		double x2 = rem.get_dimension(0).b;
//		double y1 = rem.get_dimension(1).a;
//		double y2 = rem.get_dimension(1).b;
//
//		snap_to_pixel_grid( x1, x2, ref.stref.x1, ref.pixel_scale_x );
//		snap_to_pixel_grid( y1, y2, ref.stref.y1, ref.pixel_scale_y );
//
//	//	Log::info("Remainder after snap: [%f,%f]x[%f,%f]",x1,x2,y1,y2);
//
//		// Shrink it just a little
//		// Fixme: Operator throws exception if i leave this out --> Result does not contain...
//		// But while debugging everything was fine
//		x1 = x1+ref.pixel_scale_x*0.001;
//		x2 = x2-ref.pixel_scale_x*0.001;
//		y1 = y1+ref.pixel_scale_y*0.001;
//		y2 = y2-ref.pixel_scale_y*0.001;
//
//		uint32_t width  = std::round((x2-x1) / ref.pixel_scale_x);
//		uint32_t height = std::round((y2-y1) / ref.pixel_scale_y);
//		result.push_back(
//			QueryRectangle( SpatialReference(query.epsg, x1, y1, x2, y2),
//							TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
//							QueryResolution::pixels(width, height) )
//		);
//	}
//	return result;
//}

std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries(double pixel_scale_x, double pixel_scale_y, double xref, double yref) const {
	std::vector<QueryRectangle> result;

	for ( auto &rem : remainder ) {

		double x1 = rem.get_dimension(0).a;
		double x2 = rem.get_dimension(0).b;
		double y1 = rem.get_dimension(1).a;
		double y2 = rem.get_dimension(1).b;

		QueryResolution qr = QueryResolution::none();
		if ( query.restype == QueryResolution::Type::PIXELS ) {
			// Skip useless remainders
			if ( rem.get_dimension(0).distance() < pixel_scale_x / 2 ||
				 rem.get_dimension(1).distance() < pixel_scale_y / 2)
				continue;
			// Make sure we have at least one pixel
			snap_to_pixel_grid(x1,x2,xref,pixel_scale_x);
			snap_to_pixel_grid(y1,y2,yref,pixel_scale_y);
			qr = QueryResolution::pixels( std::round((x2-x1) / pixel_scale_x),
				                          std::round((y2-y1) / pixel_scale_y) );
		}

		result.push_back(
			QueryRectangle( SpatialReference(query.epsg, x1, y1, x2, y2),
							TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
							qr )
		);
	}
	return result;
}

void PuzzleRequest::snap_to_pixel_grid( double &v1, double &v2, double ref, double scale ) const {
	if ( ref < v1 )
		v1 = ref + std::floor( (v1-ref) / scale)*scale;
	else
		v1 = ref - std::ceil( (ref-v1) / scale)*scale;
	v2 = v1 + std::ceil( (v2-v1) / scale)*scale;
}
