/*
 * shared.cpp
 *
 *  Created on: 02.03.2016
 *      Author: mika
 */

#include "cache/priv/shared.h"

#include "util/concat.h"

#include <chrono>
#include <limits>

///////////////////////////////////////////////////////////
//
// RESOLUTION INFO
//
///////////////////////////////////////////////////////////

ResolutionInfo::ResolutionInfo() :
	restype(QueryResolution::Type::NONE),
	actual_pixel_scale_x(0),
	actual_pixel_scale_y(0) {
}

ResolutionInfo::ResolutionInfo(const GridSpatioTemporalResult& result) :
	restype(QueryResolution::Type::PIXELS),
	pixel_scale_x( result.pixel_scale_x, result.pixel_scale_x*2 ),
	pixel_scale_y( result.pixel_scale_y, result.pixel_scale_y*2 ),
	actual_pixel_scale_x(result.pixel_scale_x),
	actual_pixel_scale_y(result.pixel_scale_y) {
}

ResolutionInfo::ResolutionInfo(BinaryReadBuffer& buffer) :
	restype(buffer.read<QueryResolution::Type>()), pixel_scale_x(buffer), pixel_scale_y(buffer),
	actual_pixel_scale_x(buffer.read<double>()), actual_pixel_scale_y(buffer.read<double>()) {
}

void ResolutionInfo::toStream(BinaryWriteBuffer& buffer) const {
	pixel_scale_x.toStream(buffer);
	pixel_scale_y.toStream(buffer);
	buffer.write(restype);
	buffer.write(actual_pixel_scale_x);
	buffer.write(actual_pixel_scale_y);
}

bool ResolutionInfo::matches(const QueryRectangle& query) const {
	return query.restype == restype &&
	// No resolution --> matches
    (restype == QueryResolution::Type::NONE ||
    	// Check res
	    (pixel_scale_x.contains((query.x2-query.x1) / query.xres) &&
		 pixel_scale_y.contains((query.y2-query.y1) / query.yres))
    );
}

///////////////////////////////////////////////////////////
//
// QUERY CUBE
//
///////////////////////////////////////////////////////////

QueryCube::QueryCube(const QueryRectangle& rect) : QueryCube(rect,rect) {
}

QueryCube::QueryCube(const SpatialReference& sref, const TemporalReference& tref) :
	Cube3( sref.x1, sref.x2, sref.y1, sref.y2, tref.t1,
	// Always make timespan an interval -- otherwise the volume function of cube returns 0
	// Currently only works for unix timestamps
	std::max( tref.t2, tref.t1 + 0.25 ) ), epsg(sref.epsg), timetype(tref.timetype) {
}

QueryCube::QueryCube(BinaryReadBuffer& buffer) : Cube3(buffer),
	epsg(buffer.read<epsg_t>()), timetype(buffer.read<timetype_t>()) {
}

void QueryCube::toStream(BinaryWriteBuffer& buffer) const {
	Cube3::toStream(buffer);
	buffer.write(epsg);
	buffer.write(timetype);
}

///////////////////////////////////////////////////////////
//
// CACHE CUBE
//
///////////////////////////////////////////////////////////

CacheCube::CacheCube(const SpatialReference& sref, const TemporalReference& tref) :
	QueryCube( sref, tref ) {
}

CacheCube::CacheCube(const SpatioTemporalResult& result) : CacheCube( result.stref, result.stref ) {
}

CacheCube::CacheCube(const GridSpatioTemporalResult& result) :
	QueryCube(result.stref,result.stref), resolution_info(result) {
}

CacheCube::CacheCube(const GenericPlot& result) :
	QueryCube( SpatialReference(EPSG_UNREFERENCED, -std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
			std::numeric_limits<double>::infinity(),std::numeric_limits<double>::infinity()),
		       TemporalReference(TIMETYPE_UNREFERENCED, -std::numeric_limits<double>::infinity(),std::numeric_limits<double>::infinity()) ) {
	(void) result;
}


CacheCube::CacheCube(BinaryReadBuffer& buffer) : QueryCube(buffer), resolution_info(buffer) {
}

void CacheCube::toStream(BinaryWriteBuffer& buffer) const {
	QueryCube::toStream(buffer);
	resolution_info.toStream(buffer);
}


const Interval& CacheCube::get_timespan() const {
	return get_dimension(2);
}

///////////////////////////////////////////////////////////
//
// FETCH INFO
//
///////////////////////////////////////////////////////////

FetchInfo::FetchInfo(uint64_t size, const ProfilingData &profile) :
		size(size),
		profile(profile) {
}

FetchInfo::FetchInfo(BinaryReadBuffer& buffer) :
	size(buffer.read<uint64_t>()), profile(buffer) {
}

void FetchInfo::toStream(BinaryWriteBuffer& buffer) const {
	buffer.write(size);
	profile.toStream(buffer);
}

///////////////////////////////////////////////////////////
//
// CACHE ENTRY
//
///////////////////////////////////////////////////////////

CacheEntry::CacheEntry(CacheCube bounds, uint64_t size, const ProfilingData &profile) :
	FetchInfo(size, profile),
	last_access( std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count() ),
	access_count(1),
	bounds(bounds) {
}

CacheEntry::CacheEntry(CacheCube bounds, uint64_t size, const ProfilingData &profile, uint64_t last_access, uint32_t access_count) :
	FetchInfo(size,profile),
	last_access(last_access),
	access_count(access_count),
	bounds(bounds) {
}

CacheEntry::CacheEntry(BinaryReadBuffer& buffer) :
		FetchInfo(buffer), last_access(buffer.read<uint64_t>()),
		access_count(buffer.read<uint32_t>()), bounds(buffer) {
}

void CacheEntry::toStream(BinaryWriteBuffer& buffer) const {
	FetchInfo::toStream(buffer);
	buffer.write(last_access);
	buffer.write(access_count);
	bounds.toStream(buffer);
}

std::string CacheEntry::to_string() const {
	return concat("CacheEntry[size: ", size, ",profle: [",profile.to_string(),"], last_access: ", last_access, ", access_count: ", access_count, ", bounds: ", bounds.to_string(), "]");
}

///////////////////////////////////////////////////////////
//
// NodeCacheKey
//
///////////////////////////////////////////////////////////

NodeCacheKey::NodeCacheKey(const std::string& semantic_id, uint64_t entry_id) :
	semantic_id(semantic_id), entry_id(entry_id) {
}

NodeCacheKey::NodeCacheKey(BinaryReadBuffer &buffer) :
	semantic_id( buffer.read<std::string>() ), entry_id( buffer.read<uint64_t>() ){
}

void NodeCacheKey::toStream(BinaryWriteBuffer &buffer) const {
	buffer.write(semantic_id);
	buffer.write(entry_id);
}

std::string NodeCacheKey::to_string() const {
	return concat( "NodeCacheKey[ semantic_id: ", semantic_id, ", id: ", entry_id, "]");
}

///////////////////////////////////////////////////////////
//
// TypedNodeCacheKey
//
///////////////////////////////////////////////////////////

TypedNodeCacheKey::TypedNodeCacheKey(CacheType type, const std::string& semantic_id, uint64_t entry_id) :
	NodeCacheKey(semantic_id,entry_id), type(type) {
}

TypedNodeCacheKey::TypedNodeCacheKey(BinaryReadBuffer &buffer) : NodeCacheKey(buffer), type( buffer.read<CacheType>() ) {
}

void TypedNodeCacheKey::toStream(BinaryWriteBuffer &buffer) const {
	NodeCacheKey::toStream(buffer);
	buffer.write(type);
}

std::string TypedNodeCacheKey::to_string() const {
	return concat( "TypedNodeCacheKey[ type: ", (int)type, ", semantic_id: ", semantic_id, ", id: ", entry_id, "]");
}

///////////////////////////////////////////////////////////
//
// MetaCacheEntry
//
///////////////////////////////////////////////////////////

MetaCacheEntry::MetaCacheEntry(const TypedNodeCacheKey& key, const CacheEntry& entry) :
	TypedNodeCacheKey(key), CacheEntry(entry) {
}

MetaCacheEntry::MetaCacheEntry(CacheType type, const NodeCacheKey& key, const CacheEntry& entry) :
	TypedNodeCacheKey(type,key.semantic_id,key.entry_id), CacheEntry(entry) {
}

MetaCacheEntry::MetaCacheEntry(CacheType type, const std::string semantic_id, uint64_t entry_id, const CacheEntry& entry) :
	TypedNodeCacheKey(type,semantic_id,entry_id), CacheEntry(entry) {
}

MetaCacheEntry::MetaCacheEntry(BinaryReadBuffer &buffer) :
	TypedNodeCacheKey(buffer), CacheEntry(buffer) {
}

void MetaCacheEntry::toStream(BinaryWriteBuffer &buffer) const {
	TypedNodeCacheKey::toStream(buffer);
	CacheEntry::toStream(buffer);
}

std::string MetaCacheEntry::to_string() const {
	return concat("NodeCacheRef[ key: ", TypedNodeCacheKey::to_string(), ", entry: ", CacheEntry::to_string(), "]");
}

///////////////////////////////////////////////////////////
//
// ForeignRef
//
///////////////////////////////////////////////////////////

ForeignRef::ForeignRef(const std::string& host, uint32_t port) :
	host(host), port(port) {
}

ForeignRef::ForeignRef(BinaryReadBuffer& buffer) :
	host( buffer.read<std::string>() ), port( buffer.read<uint32_t>() ) {
}

void ForeignRef::toStream(BinaryWriteBuffer& buffer) const {
	buffer.write(host);
	buffer.write(port);
}

///////////////////////////////////////////////////////////
//
// DeliveryResponse
//
///////////////////////////////////////////////////////////

DeliveryResponse::DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id) :
			ForeignRef(host,port), delivery_id(delivery_id) {
}

DeliveryResponse::DeliveryResponse(BinaryReadBuffer& buffer) :
	ForeignRef(buffer), delivery_id( buffer.read<uint64_t>()) {
}

void DeliveryResponse::toStream(BinaryWriteBuffer& buffer) const {
	ForeignRef::toStream(buffer);
	buffer.write(delivery_id);
}

std::string DeliveryResponse::to_string() const {
	return concat("DeliveryResponse[", host, ":", port, ", delivery_id: ", delivery_id, "]");
}

///////////////////////////////////////////////////////////
//
// CacheRef
//
///////////////////////////////////////////////////////////


CacheRef::CacheRef(const std::string& host, uint32_t port, uint64_t entry_id) :
	ForeignRef(host,port), entry_id(entry_id) {
}

CacheRef::CacheRef(BinaryReadBuffer& buffer) : ForeignRef(buffer), entry_id( buffer.read<uint64_t>() ) {
}

void CacheRef::toStream(BinaryWriteBuffer& buffer) const {
	ForeignRef::toStream(buffer);
	buffer.write(entry_id);
}

std::string CacheRef::to_string() const {
	return concat( "CacheRef[", host, ":", port, ", entry_id: ", entry_id, "]");
}
