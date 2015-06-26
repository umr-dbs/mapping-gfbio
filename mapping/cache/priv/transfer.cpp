/*
 * transfer.cpp
 *
 *  Created on: 11.06.2015
 *      Author: mika
 */

#include "cache/priv/transfer.h"
#include "cache/common.h"

#include <geos/io/WKBWriter.h>
#include <geos/io/WKBReader.h>

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
	ss << "  query: " << Common::qr_to_string(query);
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
	ss << "  query: " << Common::qr_to_string(query) << std::endl;
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
	ss << "  query: " << Common::qr_to_string(query) << std::endl;
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

QueryRectangle PuzzleRequest::get_remainder_query(double xres, double yres) const {
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

	// Enlarge by 2 pixels in each direction
	x1 -= (2*xres);
	x2 += (2*xres);
	y1 -= (2*yres);
	y2 += (2*yres);

	uint32_t width  = std::floor( (x2-x1) / xres );
	uint32_t height = std::floor( (y2-y1) / yres );
	return QueryRectangle( query.timestamp, x1, y1, x2, y2, width, height, query.epsg );
}

//
// Super with-raster
//

WithRaster::WithRaster(QM qm) :
	query_mode(qm) {
}

WithRaster::WithRaster(BinaryStream& stream) {
	uint8_t input;
	stream.read(&input);
	query_mode = (input==1) ? QM::EXACT : QM::LOOSE;
}

WithRaster::~WithRaster() {
	// Nothing to do
}

void WithRaster::toStream(BinaryStream& stream) const {
	uint8_t out = (query_mode == QM::EXACT) ? 1 : 0;
	stream.write(out);
}

std::string WithRaster::to_string() const {
	std::ostringstream ss;
	std::string mode = (query_mode == QM::EXACT) ? "EXACT" : "LOOSE";
	ss << "  query_mode: " <<  mode << std::endl;
	return ss.str();
}


//
// Raster stuff
//

RasterBaseRequest::RasterBaseRequest(const std::string& sem_id, const QueryRectangle& rect, QM qm) :
	BaseRequest(sem_id,rect), WithRaster(qm) {
}

RasterBaseRequest::RasterBaseRequest( BinaryStream &stream ) : BaseRequest(stream), WithRaster(stream) {};

RasterBaseRequest::RasterBaseRequest( const RasterBaseRequest &r ) : BaseRequest(r), WithRaster(r) {};
RasterBaseRequest::RasterBaseRequest( RasterBaseRequest &&r ) : BaseRequest(r), WithRaster(r) {};

RasterBaseRequest& RasterBaseRequest::operator=( const RasterBaseRequest & r ) {
	BaseRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

RasterBaseRequest& RasterBaseRequest::operator=( RasterBaseRequest &&r ) {
	BaseRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

void RasterBaseRequest::toStream( BinaryStream &stream ) const {
	BaseRequest::toStream(stream);
	WithRaster::toStream(stream);
}

std::string RasterBaseRequest::to_string() const {
	std::ostringstream ss;
	ss << BaseRequest::to_string() << std::endl << WithRaster::to_string();
	return ss.str();
}


RasterDeliveryRequest::RasterDeliveryRequest( const std::string &sem_id, const QueryRectangle &rect, uint64_t entry_id, QM qm ) :
	DeliveryRequest(sem_id,rect,entry_id), WithRaster(qm) {
};

RasterDeliveryRequest::RasterDeliveryRequest( BinaryStream &stream ) :
	DeliveryRequest(stream), WithRaster(stream) {
};

RasterDeliveryRequest::RasterDeliveryRequest( const RasterDeliveryRequest &r ) :
	DeliveryRequest(r), WithRaster(r) {
};

RasterDeliveryRequest::RasterDeliveryRequest( RasterDeliveryRequest &&r ) :
	DeliveryRequest(r), WithRaster(r) {
};


RasterDeliveryRequest& RasterDeliveryRequest::operator=( const RasterDeliveryRequest & r ) {
	DeliveryRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

RasterDeliveryRequest& RasterDeliveryRequest::operator=( RasterDeliveryRequest &&r ) {
	DeliveryRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

void RasterDeliveryRequest::toStream( BinaryStream &stream ) const {
	DeliveryRequest::toStream(stream);
	WithRaster::toStream(stream);
}

std::string RasterDeliveryRequest::to_string() const {
	std::ostringstream ss;
	ss << DeliveryRequest::to_string() << std::endl << WithRaster::to_string();
	return ss.str();
}


RasterPuzzleRequest::RasterPuzzleRequest( const std::string &sem_id, const QueryRectangle &rect, const GeomP &covered, const GeomP &remainder, const std::vector<CacheRef> &parts, QM qm ) :
	PuzzleRequest(sem_id,rect,covered,remainder,parts), WithRaster(qm) {
};

RasterPuzzleRequest::RasterPuzzleRequest( BinaryStream &stream ) :
	PuzzleRequest(stream), WithRaster(stream) {
};

RasterPuzzleRequest::RasterPuzzleRequest( const RasterPuzzleRequest &r ) :
	PuzzleRequest(r), WithRaster(r) {
};

RasterPuzzleRequest::RasterPuzzleRequest( RasterPuzzleRequest &&r ) :
	PuzzleRequest(r), WithRaster(r) {
};

RasterPuzzleRequest& RasterPuzzleRequest::operator=( const RasterPuzzleRequest & r ) {
	PuzzleRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

RasterPuzzleRequest& RasterPuzzleRequest::operator=( RasterPuzzleRequest &&r ) {
	PuzzleRequest::operator =(r);
	WithRaster::operator=(r);
	return *this;
}

void RasterPuzzleRequest::toStream( BinaryStream &stream ) const {
	PuzzleRequest::toStream(stream);
	WithRaster::toStream(stream);
}

std::string RasterPuzzleRequest::to_string() const {
	std::ostringstream ss;
	ss << PuzzleRequest::to_string() << std::endl << WithRaster::to_string();
	return ss.str();
}
