/*
 * cache_stats.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/priv/cache_stats.h"
#include "util/concat.h"

Capacity::Capacity(size_t raster_cache_size, size_t raster_cache_used) :
	raster_cache_total(raster_cache_size), raster_cache_used(raster_cache_used) {
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

double Capacity::get_raster_usage() const {
	return (double) raster_cache_used / (double) raster_cache_total;
}

void Capacity::toStream(BinaryStream& stream) const {
	stream.write( static_cast<uint64_t>(raster_cache_total) );
	stream.write( static_cast<uint64_t>(raster_cache_used) );
}

std::string Capacity::to_string() const {
	return concat("Capacity[ Raster: ", raster_cache_used, "/", raster_cache_total, "]");
}

//
// Handshake
//

NodeHandshake::NodeHandshake( const std::string &host, uint32_t port, const Capacity &cap, std::vector<NodeCacheRef> raster_entries ) :
	Capacity(cap), host(host), port(port), raster_entries(raster_entries) {
}

NodeHandshake::NodeHandshake(BinaryStream& stream) : Capacity(stream) {
	uint64_t r_size;
	stream.read(&host);
	stream.read(&port);

	stream.read(&r_size);
	raster_entries.reserve(r_size);
	for ( uint64_t i = 0; i < r_size; i++ )
		raster_entries.push_back( NodeCacheRef(stream) );
}

NodeHandshake::~NodeHandshake() {
}

void NodeHandshake::toStream(BinaryStream& stream) const {
	Capacity::toStream(stream);
	stream.write(host);
	stream.write(port);
	stream.write( static_cast<uint64_t>(raster_entries.size()) );
	for ( auto &e : raster_entries )
		e.toStream(stream);
}

const std::vector<NodeCacheRef>& NodeHandshake::get_raster_entries() const {
	return raster_entries;
}

std::string NodeHandshake::to_string() const {
	return concat("NodeHandshake[host: ", host, ", port: ", port, ", "
		"capacity: ", Capacity::to_string(), ", rasters: ", raster_entries.size(), "]");
}

//
// Stats
//

NodeEntryStats::NodeEntryStats(uint64_t id, time_t last_access, uint32_t access_count) :
	entry_id(id), last_access(last_access), access_count(access_count) {
}

NodeEntryStats::NodeEntryStats(BinaryStream& stream) {
	stream.read(&entry_id);
	stream.read(&last_access);
	stream.read(&access_count);
}

void NodeEntryStats::toStream(BinaryStream& stream) const {
	stream.write(entry_id);
	stream.write(last_access);
	stream.write(access_count);
}

CacheStats::CacheStats() {
}

CacheStats::CacheStats(BinaryStream& stream) {
	uint64_t size;
	uint64_t v_size;
	stream.read(&size);

	stats.reserve(size);

	for ( size_t i = 0; i < size; i++ ) {
		std::string semantic_id;
		stream.read(&semantic_id);
		stream.read(&v_size);
		std::vector<NodeEntryStats> items;
		items.reserve(v_size);

		for ( size_t j = 0; j < v_size; j++ ) {
			items.push_back( NodeEntryStats(stream) );
		}
		stats.emplace( semantic_id, items );
	}
}

void CacheStats::toStream(BinaryStream& stream) const {
	stream.write(static_cast<uint64_t>(stats.size()));
	for ( auto &e : stats ) {
		stream.write(e.first);
		stream.write(static_cast<uint64_t>(e.second.size()));
		for ( auto &s : e.second )
			s.toStream(stream);
	}
}

void CacheStats::add_stats(const std::string &semantic_id, NodeEntryStats stats) {
	try {
		this->stats.at( semantic_id ).push_back(stats);
	} catch ( const std::out_of_range &oor ) {
		std::vector<NodeEntryStats> sv;
		sv.push_back(stats);
		this->stats.emplace( semantic_id, sv );
	}
}

const std::unordered_map<std::string, std::vector<NodeEntryStats> >& CacheStats::get_stats() const {
	return stats;
}

NodeStats::NodeStats(const Capacity &capacity, CacheStats raster_stats) :
	Capacity(capacity), raster_stats(raster_stats) {
}

NodeStats::NodeStats(BinaryStream& stream) : Capacity(stream), raster_stats(stream) {
}

NodeStats::~NodeStats() {
}

void NodeStats::toStream(BinaryStream& stream) const {
	Capacity::toStream(stream);
	raster_stats.toStream(stream);
}

