/*
 * requests.cpp
 *
 *  Created on: 02.03.2016
 *      Author: mika
 */

#include "cache/priv/requests.h"
#include "cache/common.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include <sstream>

///////////////////////////////////////////////////////////
//
// BaseRequest
//
///////////////////////////////////////////////////////////

BaseRequest::BaseRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect) :
	type(type),
	semantic_id(sem_id),
	query(rect) {
}

BaseRequest::BaseRequest(BinaryReadBuffer& buffer) : type(buffer.read<CacheType>() ),
		semantic_id(buffer.read<std::string>()), query(buffer)  {
}

void BaseRequest::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	buffer.write(type);
	buffer.write(semantic_id, is_persistent_memory);
	buffer.write(query, is_persistent_memory);
}

std::string BaseRequest::to_string() const {
	std::ostringstream ss;
	ss << "BaseRequest:" << std::endl;
	ss << "  type: " << (int) type << std::endl;
	ss << "  semantic_id: " << semantic_id << std::endl;
	ss << "  query: " << CacheCommon::qr_to_string(query);
	return ss.str();
}

///////////////////////////////////////////////////////////
//
// DeliveryRequest
//
///////////////////////////////////////////////////////////
DeliveryRequest::DeliveryRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect, uint64_t entry_id) :
	BaseRequest(type,sem_id,rect), entry_id(entry_id) {
}

DeliveryRequest::DeliveryRequest(BinaryReadBuffer& buffer) :
	BaseRequest(buffer), entry_id(buffer.read<uint64_t>()) {
}

void DeliveryRequest::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	BaseRequest::serialize(buffer, is_persistent_memory);
	buffer.write(entry_id);
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

///////////////////////////////////////////////////////////
//
// PuzzleRequest
//
///////////////////////////////////////////////////////////

PuzzleRequest::PuzzleRequest(CacheType type, const std::string& sem_id, const QueryRectangle& rect,
	const std::vector<Cube<3>> &remainder, const std::vector<CacheRef>& parts) :
	BaseRequest(type,sem_id,rect),
	parts(parts),
	remainder( remainder ) {
}

PuzzleRequest::PuzzleRequest(BinaryReadBuffer& buffer) :
	BaseRequest(buffer) {

	uint64_t v_size = buffer.read<uint64_t>();
	remainder.reserve(v_size);
	for ( uint64_t i = 0; i < v_size; i++ ) {
		remainder.push_back( Cube<3>(buffer) );
	}


	buffer.read(&v_size);
	parts.reserve(v_size);
	for ( uint64_t i = 0; i < v_size; i++ ) {
		parts.push_back( CacheRef(buffer) );
	}
}

void PuzzleRequest::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	BaseRequest::serialize(buffer, is_persistent_memory);

	buffer.write( static_cast<uint64_t>(remainder.size()));
	for ( auto &rem : remainder )
		buffer.write(rem);

	buffer.write( static_cast<uint64_t>(parts.size()));
	for ( auto &cr : parts )
		buffer.write(cr);
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

template<class T>
std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries(
		const T& ref_result) const {
	(void) ref_result;
	std::vector<QueryRectangle> result;
	result.reserve(remainder.size());

	for ( auto &rem : remainder ) {
		result.push_back( QueryRectangle( SpatialReference(query.epsg, rem.get_dimension(0).a, rem.get_dimension(1).
												a, rem.get_dimension(0).b, rem.get_dimension(1).b),
											TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
											QueryResolution::none() ) );
	}
	return result;
}

template<>
std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries(
		const GenericRaster& ref_result) const {

	std::vector<QueryRectangle> result;
	result.reserve(remainder.size());

	for ( auto &rem : remainder ) {
		double x1 = rem.get_dimension(0).a;
		double x2 = rem.get_dimension(0).b;
		double y1 = rem.get_dimension(1).a;
		double y2 = rem.get_dimension(1).b;


		// Skip useless remainders
		if ( rem.get_dimension(0).distance() < ref_result.pixel_scale_x / 2 ||
			 rem.get_dimension(1).distance() < ref_result.pixel_scale_y / 2)
			continue;
		// Make sure we have at least one pixel
		snap_to_pixel_grid(x1,x2,ref_result.stref.x1,ref_result.pixel_scale_x);
		snap_to_pixel_grid(y1,y2,ref_result.stref.y1,ref_result.pixel_scale_y);


		result.push_back( QueryRectangle( SpatialReference(query.epsg, x1,y1,x2,y2),
										  TemporalReference(query.timetype, rem.get_dimension(2).a,rem.get_dimension(2).b),
										  QueryResolution::pixels( std::round((x2-x1) / ref_result.pixel_scale_x),
																   std::round((y2-y1) / ref_result.pixel_scale_y) ) ) );
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

size_t PuzzleRequest::get_num_remainders() const {
	return remainder.size();
}

bool PuzzleRequest::has_remainders() const {
	return !remainder.empty();
}

template std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries<PointCollection>(const PointCollection&) const;
template std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries<LineCollection>(const LineCollection&) const;
template std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries<PolygonCollection>(const PolygonCollection&) const;
template std::vector<QueryRectangle> PuzzleRequest::get_remainder_queries<GenericPlot>(const GenericPlot&) const;


