/*
 * cache_stats.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/priv/cache_stats.h"
#include "util/concat.h"

Capacity::Capacity(size_t raster_cache_size) :
	raster_cache_total(raster_cache_size), raster_cache_used(0) {
}

Capacity::Capacity(BinaryStream& stream) {
	uint64_t tmp;
	stream.read(&tmp);
	raster_cache_total = tmp;
	stream.read(&tmp);
	raster_cache_used = tmp;
}

Capacity::~Capacity() {
}

size_t Capacity::get_raster_cache_total() {
	return raster_cache_total;
}

size_t Capacity::get_raster_cache_used() {
	return raster_cache_used;
}

void Capacity::add_raster(size_t size) {
	raster_cache_used += size;
}

void Capacity::remove_raster(size_t size) {
	raster_cache_used -= size;
}

double Capacity::get_raster_usage() {
	return (double) raster_cache_used / (double) raster_cache_total;
}

void Capacity::toStream(BinaryStream& stream) {
	stream.write( static_cast<uint64_t>(raster_cache_total) );
	stream.write( static_cast<uint64_t>(raster_cache_used) );
}

std::string Capacity::to_string() {
	return concat("Capacity[ Raster: ", raster_cache_used, "/", raster_cache_total, "]");
}


NodeStats::NodeStats(const Capacity &capacity) : Capacity(capacity){
}

NodeStats::NodeStats(BinaryStream& stream) : Capacity(stream) {
}

NodeStats::~NodeStats() {
}

NodeHandshake::NodeHandshake( const std::string &host, uint32_t port, const NodeStats &stats ) :
	NodeStats(stats), host(host), port(port) {
}

NodeHandshake::NodeHandshake(BinaryStream& stream) : NodeStats(stream) {
	stream.read(&host);
	stream.read(&port);
}

NodeHandshake::~NodeHandshake() {
}

void NodeHandshake::toStream(BinaryStream& stream) {
	NodeStats::toStream(stream);
	stream.write(host);
	stream.write(port);
}

std::string NodeHandshake::to_string() {
	return concat("NodeHandshake[host: ", host, ", port: ", port, ", ", NodeStats::to_string(), "]");
}

AccessInfo::AccessInfo(uint64_t id) : id(id), count(1), timestamp(time(nullptr)) {
}

AccessInfo::AccessInfo(BinaryStream& stream) {
	uint64_t ts;
	stream.read(&id);
	stream.read(&ts);
	stream.read(&count);
	timestamp = ts;
}

void AccessInfo::accessed() {
	count++;
	time(&timestamp);
}

void AccessInfo::toStream(BinaryStream& stream) {
	stream.write(id);
	stream.write( static_cast<uint64_t>(timestamp) );
	stream.write( count );
}
