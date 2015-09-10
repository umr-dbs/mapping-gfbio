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

ForeignRef::~ForeignRef() {
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

DeliveryResponse::~DeliveryResponse() {
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

CacheRef::~CacheRef() {
}

std::string CacheRef::to_string() const {
	std::ostringstream ss;
	ss << "CacheRef[" << host << ":" << port << ", entry_id: " << entry_id << "]";
	return ss.str();
}

//
// Requests
//


BaseRequest::BaseRequest(const std::string& sem_id, const QueryRectangle& rect) :
	semantic_id(sem_id),
	query(rect) {
}

BaseRequest::BaseRequest(BinaryStream& stream) : query(stream) {
	stream.read(&semantic_id);
}

BaseRequest::~BaseRequest() {
	// Nothing to do
}

void BaseRequest::toStream(BinaryStream& stream) const {
	query.toStream(stream);
	stream.write(semantic_id);
}

std::string BaseRequest::to_string() const {
	std::ostringstream ss;
	ss << "BaseRequest:" << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query);
	return ss.str();
}

//
// Delivery request
//

DeliveryRequest::DeliveryRequest(const std::string& sem_id, const QueryRectangle& rect, uint64_t entry_id) :
	BaseRequest(sem_id,rect), entry_id(entry_id) {
}

DeliveryRequest::DeliveryRequest(BinaryStream& stream) :
	BaseRequest(stream) {
	stream.read(&entry_id);
}

DeliveryRequest::~DeliveryRequest() {
	// Nothing to do
}

void DeliveryRequest::toStream(BinaryStream& stream) const {
	BaseRequest::toStream(stream);
	stream.write(entry_id);
}

std::string DeliveryRequest::to_string() const {
	std::ostringstream ss;
	ss << "DeliveryRequest:" << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query) << std::endl;
	ss << "  entry_id: " << entry_id;
	return ss.str();
}

//
// Puzzle-Request
//

PuzzleRequest::PuzzleRequest(const std::string& sem_id, const QueryRectangle& rect, const GeomP& covered,
		const GeomP& remainder, const std::vector<CacheRef>& parts) :
	BaseRequest(sem_id,rect),
	covered( GeomP( covered->clone() ) ),
	remainder( GeomP( remainder->clone() ) ),
	parts(parts) {
}

PuzzleRequest::PuzzleRequest(BinaryStream& stream) :
	BaseRequest(stream) {

	std::istringstream buffer;
	geos::io::WKBReader reader;

	std::string tmp;
	stream.read(&tmp);
	buffer.str(tmp);
	covered.reset(reader.read(buffer));

	stream.read(&tmp);
	buffer.str(tmp);
	remainder.reset(reader.read(buffer));

	uint64_t v_size;
	stream.read(&v_size);

	parts.reserve(v_size);
	for ( uint64_t i = 0; i < v_size; i++ ) {
		parts.push_back( CacheRef(stream) );
	}
}

PuzzleRequest::PuzzleRequest(const PuzzleRequest& r) :
	BaseRequest(r),
	covered( GeomP( r.covered->clone() ) ),
	remainder( GeomP( r.remainder->clone() ) ),
	parts( r.parts ) {
}

PuzzleRequest::PuzzleRequest(PuzzleRequest&& r) :
	BaseRequest(r),
	covered( std::move(r.covered) ),
	remainder( std::move(r.remainder) ),
	parts( std::move(r.parts) ){
}

PuzzleRequest::~PuzzleRequest() {
	// Nothing to do
}

PuzzleRequest& PuzzleRequest::operator =(const PuzzleRequest& r) {
	BaseRequest::operator=(r);
	covered = GeomP( r.covered->clone() );
	remainder = GeomP( r.remainder->clone() );
	parts = r.parts;
	return *this;
}

PuzzleRequest& PuzzleRequest::operator =(PuzzleRequest&& r) {
	BaseRequest::operator=(r);
	covered = std::move(r.covered);
	remainder = std::move(r.remainder);
	parts = std::move(r.parts);
	return *this;
}

void PuzzleRequest::toStream(BinaryStream& stream) const {
	BaseRequest::toStream(stream);

	std::ostringstream buffer;
	geos::io::WKBWriter writer;

	writer.write( *covered, buffer );
	stream.write(buffer.str());

	buffer.str("");

	writer.write( *remainder, buffer );
	stream.write(buffer.str());

	uint64_t v_size = parts.size();
	stream.write(v_size);
	for ( auto &cr : parts ) {
		cr.toStream(stream);
	}
}

std::string PuzzleRequest::to_string() const {
	std::ostringstream ss;
	ss << "PuzzleRequest:" << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query) << std::endl;
	ss << "  covered: " << covered->toString() << std::endl;
	ss << "  remainder: " << remainder->toString() << std::endl;
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
QueryRectangle PuzzleRequest::get_remainder_query(const GridSpatioTemporalResult &ref) const {
	double x1 = DoubleInfinity, x2 = DoubleNegInfinity, y1 = DoubleInfinity, y2 = DoubleNegInfinity;
	auto cos = remainder->getCoordinates();


	for ( size_t i = 0; i < cos->getSize(); i++ ) {
		auto &c = cos->getAt(i);
		x1 = std::min(x1,c.x);
		x2 = std::max(x2,c.x);
		y1 = std::min(y1,c.y);
		y2 = std::max(y2,c.y);
	}
	delete cos;

//	Log::info("Remainder before snap: [%f,%f]x[%f,%f]",x1,x2,y1,y2);

	snap_to_pixel_grid( x1, x2, ref.stref.x1, ref.pixel_scale_x );
	snap_to_pixel_grid( y1, y2, ref.stref.y1, ref.pixel_scale_y );

//	Log::info("Remainder after snap: [%f,%f]x[%f,%f]",x1,x2,y1,y2);

	// Shrink it just a little
	// Fixme: Operator throws exception if i leave this out --> Result does not contain...
	// But while debugging everything was fine
	x1 = x1+ref.pixel_scale_x*0.001;
	x2 = x2-ref.pixel_scale_x*0.001;
	y1 = y1+ref.pixel_scale_y*0.001;
	y2 = y2-ref.pixel_scale_y*0.001;

	uint32_t width  = std::round((x2-x1) / ref.pixel_scale_x);
	uint32_t height = std::round((y2-y1) / ref.pixel_scale_y);

	return QueryRectangle(SpatialReference(query.epsg, x1, y1, x2, y2), query, QueryResolution::pixels(width, height));
}

void PuzzleRequest::snap_to_pixel_grid( double &v1, double &v2, double ref, double scale ) const {
	if ( ref < v1 )
		v1 = ref + std::floor( (v1-ref) / scale)*scale;
	else
		v1 = ref - std::ceil( (ref-v1) / scale)*scale;
	v2 = v1 + std::ceil( (v2-v1) / scale)*scale;
}
