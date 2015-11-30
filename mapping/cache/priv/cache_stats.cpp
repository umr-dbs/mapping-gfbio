/*
 * cache_stats.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/priv/cache_stats.h"
#include "util/concat.h"

Capacity::Capacity(uint64_t raster_cache_total, uint64_t raster_cache_used,
	  uint64_t point_cache_total, uint64_t point_cache_used,
	  uint64_t line_cache_total, uint64_t line_cache_used,
	  uint64_t polygon_cache_total, uint64_t polygon_cache_used,
	  uint64_t plot_cache_total, uint64_t plot_cache_used) :
	raster_cache_total(raster_cache_total), raster_cache_used(raster_cache_used),
	point_cache_total(point_cache_total), point_cache_used(point_cache_used),
	line_cache_total(line_cache_total), line_cache_used(line_cache_used),
	polygon_cache_total(polygon_cache_total), polygon_cache_used(polygon_cache_used),
	plot_cache_total(plot_cache_total), plot_cache_used(plot_cache_used) {
}

Capacity::Capacity(BinaryStream& stream) {
	stream.read(&raster_cache_total);
	stream.read(&raster_cache_used);
	stream.read(&point_cache_total);
	stream.read(&point_cache_used);
	stream.read(&line_cache_total);
	stream.read(&line_cache_used);
	stream.read(&polygon_cache_total);
	stream.read(&polygon_cache_used);
	stream.read(&plot_cache_total);
	stream.read(&plot_cache_used);
}

void Capacity::toStream(BinaryStream& stream) const {
	stream.write( raster_cache_total );
	stream.write( raster_cache_used );
	stream.write( point_cache_total );
	stream.write( point_cache_used );
	stream.write( line_cache_total );
	stream.write( line_cache_used );
	stream.write( polygon_cache_total );
	stream.write( polygon_cache_used );
	stream.write( plot_cache_total );
	stream.write( plot_cache_used );
}

std::string Capacity::to_string() const {
	return concat("Capacity[ ", "",
		"Raster: ", raster_cache_used, "/", raster_cache_total,
		", Point: ", point_cache_used, "/", point_cache_total,
		", Line: ", line_cache_used, "/", line_cache_total,
		", Polygon: ", polygon_cache_used, "/", polygon_cache_total,
		", Plot: ", plot_cache_used, "/", plot_cache_total, "]");
}

//
// Handshake
//

NodeHandshake::NodeHandshake( uint32_t port, const Capacity &cap, std::vector<NodeCacheRef> entries ) :
	Capacity(cap), port(port), entries(entries) {
}

NodeHandshake::NodeHandshake(BinaryStream& stream) : Capacity(stream) {
	uint64_t r_size;
	stream.read(&port);

	stream.read(&r_size);
	entries.reserve(r_size);
	for ( uint64_t i = 0; i < r_size; i++ )
		entries.push_back( NodeCacheRef(stream) );
}

void NodeHandshake::toStream(BinaryStream& stream) const {
	Capacity::toStream(stream);
	stream.write(port);
	stream.write( static_cast<uint64_t>(entries.size()) );
	for ( auto &e : entries )
		e.toStream(stream);
}

const std::vector<NodeCacheRef>& NodeHandshake::get_entries() const {
	return entries;
}

std::string NodeHandshake::to_string() const {
	return concat("NodeHandshake[port: ", port, ", "
		"capacity: ", Capacity::to_string(), ", entries: ", entries.size(), "]");
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

CacheStats::CacheStats(CacheType type) : type(type) {
}

CacheStats::CacheStats(BinaryStream& stream) {
	stream.read(&type);
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
	stream.write(type);
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

NodeStats::NodeStats(const Capacity &capacity, std::vector<CacheStats> stats) :
	Capacity(capacity), stats(stats) {
}

NodeStats::NodeStats(BinaryStream& stream) : Capacity(stream) {
	uint64_t ssize;
	stream.read(&ssize);
	stats.reserve(ssize);
	for ( uint64_t i = 0; i < ssize; i++ )
		stats.push_back( CacheStats(stream) );
}

void NodeStats::toStream(BinaryStream& stream) const {
	Capacity::toStream(stream);
	stream.write(static_cast<uint64_t>(stats.size()));
	for ( auto &e : stats ) {
		e.toStream(stream);
	}
}

