/*
 * cache_stats.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/priv/cache_stats.h"
#include "util/concat.h"


///////////////////////////////////////////////////////////
//
// ENTRY-STATS
//
///////////////////////////////////////////////////////////

NodeEntryStats::NodeEntryStats(uint64_t id, uint64_t last_access, uint32_t access_count) :
	entry_id(id), last_access(last_access), access_count(access_count) {
}

NodeEntryStats::NodeEntryStats(BinaryReadBuffer& buffer) :
	entry_id( buffer.read<uint64_t>() ),
	last_access( buffer.read<uint64_t>() ),
	access_count( buffer.read<uint32_t>() ) {
}

void NodeEntryStats::serialize(BinaryWriteBuffer& buffer, bool) const {
	buffer.write(entry_id);
	buffer.write(last_access);
	buffer.write(access_count);
}

///////////////////////////////////////////////////////////
//
// HandshakeEntry
//
///////////////////////////////////////////////////////////
HandshakeEntry::HandshakeEntry(uint64_t entry_id, const CacheEntry& entry) : CacheEntry(entry), entry_id(entry_id) {
}

HandshakeEntry::HandshakeEntry(BinaryReadBuffer& buffer) : CacheEntry(buffer), entry_id(buffer.read<uint64_t>()){
}

void HandshakeEntry::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	CacheEntry::serialize(buffer, is_persistent_memory);
	buffer << entry_id;
}


CacheUsage::CacheUsage(CacheType type, uint64_t capacity_total,
		uint64_t capacity_used) : type(type), capacity_total(capacity_total), capacity_used(capacity_used) {
}

CacheUsage::CacheUsage(BinaryReadBuffer& buffer) :
		type(buffer.read<CacheType>()),
		capacity_total(buffer.read<uint64_t>()),
		capacity_used(buffer.read<uint64_t>()){
}

void CacheUsage::serialize(BinaryWriteBuffer& buffer, bool) const {
	buffer.write(type);
	buffer.write(capacity_total);
	buffer.write(capacity_used);
}

double CacheUsage::get_ratio() const {
	return (capacity_total != 0) ? (double) capacity_used/ (double) capacity_total : 1;
}

///////////////////////////////////////////////////////////
//
// CACHE-CONTENT
//
///////////////////////////////////////////////////////////
template<class T>
CacheContent<T>::CacheContent(CacheType type, uint64_t capacity_total,
		uint64_t capacity_used) : CacheUsage(type,capacity_total,capacity_used) {
}

template<class T>
CacheContent<T>::CacheContent(BinaryReadBuffer& buffer) : CacheUsage(buffer) {

	uint64_t size;
	uint64_t v_size;
	std::string semantic_id;

	buffer.read(&size);

	for ( size_t i = 0; i < size; i++ ) {
		buffer.read(&semantic_id);
		buffer.read(&v_size);
		std::vector<T> elems;
		elems.reserve(v_size);

		for ( size_t j = 0; j < v_size; j++ ) {
			elems.push_back( T(buffer) );
		}
		items.emplace( semantic_id, std::move(elems) );
	}
}

template<class T>
void CacheContent<T>::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	CacheUsage::serialize(buffer, is_persistent_memory);

	buffer.write(static_cast<uint64_t>(items.size()));
	for ( auto &e : items ) {
		buffer.write(e.first);
		buffer.write(static_cast<uint64_t>(e.second.size()));
		for ( auto & nes : e.second )
			buffer.write(nes);
	}
}

template<class T>
void CacheContent<T>::add_item(const std::string& semantic_id, T item) {
	try {
		items.at( semantic_id ).push_back(item);
	} catch ( const std::out_of_range &oor ) {
		items.emplace( semantic_id, std::vector<T>{item} );
	}
}

template<class T>
const std::map<std::string, std::vector<T> >& CacheContent<T>::get_items() const {
	return items;
}

///////////////////////////////////////////////////////////
//
// CACHE-STATS
//
///////////////////////////////////////////////////////////

CacheStats::CacheStats(CacheType type, uint64_t capacity_total, uint64_t capacity_used) :
	CacheContent(type,capacity_total,capacity_used) {
}

CacheStats::CacheStats(BinaryReadBuffer& buffer) : CacheContent(buffer) {
}

CacheHandshake::CacheHandshake(CacheType type, uint64_t capacity_total,
		uint64_t capacity_used) : CacheContent(type,capacity_total,capacity_used) {
}

CacheHandshake::CacheHandshake(BinaryReadBuffer& buffer) : CacheContent(buffer) {
}

///////////////////////////////////////////////////////////
//
// QUERY STATS
//
///////////////////////////////////////////////////////////

QueryStats::QueryStats() : single_local_hits(0), multi_local_hits(0), multi_local_partials(0),
	single_remote_hits(0), multi_remote_hits(0), multi_remote_partials(0), misses(0), queries(0), ratios(0) {
}

QueryStats::QueryStats(BinaryReadBuffer& buffer) :
	single_local_hits(buffer.read<uint32_t>()),
	multi_local_hits(buffer.read<uint32_t>()),
	multi_local_partials(buffer.read<uint32_t>()),
	single_remote_hits(buffer.read<uint32_t>()),
	multi_remote_hits(buffer.read<uint32_t>()),
	multi_remote_partials(buffer.read<uint32_t>()),
	misses(buffer.read<uint32_t>()),
	queries(buffer.read<uint64_t>()),
	ratios(buffer.read<double>()) {
}

QueryStats QueryStats::operator +(const QueryStats& stats) const {
	QueryStats res(*this);
	res.single_local_hits += stats.single_local_hits;
	res.multi_local_hits += stats.multi_local_hits;
	res.multi_local_partials += stats.multi_local_partials;
	res.single_remote_hits += stats.single_remote_hits;
	res.multi_remote_hits += stats.multi_remote_hits;
	res.multi_remote_partials += stats.multi_remote_partials;
	res.misses += stats.misses;
	res.queries += stats.queries;
	res.ratios += stats.ratios;
	return res;
}

QueryStats& QueryStats::operator +=(const QueryStats& stats) {
	single_local_hits += stats.single_local_hits;
	multi_local_hits += stats.multi_local_hits;
	multi_local_partials += stats.multi_local_partials;
	single_remote_hits += stats.single_remote_hits;
	multi_remote_hits += stats.multi_remote_hits;
	multi_remote_partials += stats.multi_remote_partials;
	misses += stats.misses;
	queries += stats.queries;
	ratios += stats.ratios;
	return *this;
}

void QueryStats::serialize(BinaryWriteBuffer& buffer, bool) const {
	buffer << single_local_hits << multi_local_hits << multi_local_partials;
	buffer << single_remote_hits << multi_remote_hits << multi_remote_partials;
	buffer << misses << queries << ratios;
}

void QueryStats::add_query(double ratio) {
	ratios += ratio;
	queries++;
}

double QueryStats::get_hit_ratio() const {
	return (queries > 0) ? (ratios/queries) : 0;
}

void QueryStats::reset() {
	single_local_hits = 0;
	multi_local_hits = 0;
	multi_local_partials = 0;
	single_remote_hits = 0;
	multi_remote_hits = 0;
	multi_remote_partials = 0;
	misses = 0;
	queries = 0;
	ratios = 0;
}

std::string QueryStats::to_string() const {
	std::ostringstream ss;
	ss << "QueryStats:" << std::endl;
	ss << "  local single hits : " << single_local_hits << std::endl;
	ss << "  local multi hits  : " << multi_local_hits << std::endl;
	ss << "  local partials    : " << multi_local_partials << std::endl;
	ss << "  remote single hits: " << single_remote_hits << std::endl;
	ss << "  remote multi hits : " << multi_remote_hits << std::endl;
	ss << "  remote partials   : " << multi_remote_partials << std::endl;
	ss << "  misses            : " << misses << std::endl;
	ss << "  hit-ratio         : " << (ratios / queries);
	return ss.str();
}

///////////////////////////////////////////////////////////
//
// NODE-STATS
//
///////////////////////////////////////////////////////////

NodeStats::NodeStats(const QueryStats &query_stats, std::vector<CacheStats> &&stats) :
	query_stats(query_stats), stats(stats) {
}

NodeStats::NodeStats(BinaryReadBuffer& buffer) : query_stats(buffer) {
	uint64_t ssize = buffer.read<uint64_t>();
	stats.reserve(ssize);
	for ( uint64_t i = 0; i < ssize; i++ )
		stats.push_back( CacheStats(buffer) );
}

void NodeStats::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	buffer.write(query_stats);
	buffer.write(static_cast<uint64_t>(stats.size()));
	for ( auto &e : stats ) {
		buffer.write(e);
	}
}

///////////////////////////////////////////////////////////
//
// HANDSHAKE
//
///////////////////////////////////////////////////////////

NodeHandshake::NodeHandshake( uint32_t port, std::vector<CacheHandshake> &&entries ) :
	port(port), data(entries) {
}

NodeHandshake::NodeHandshake(BinaryReadBuffer& buffer) {
	buffer.read(&port);
	uint64_t r_size = buffer.read<uint64_t>();

	data.reserve(r_size);
	for ( uint64_t i = 0; i < r_size; i++ )
		data.push_back( CacheHandshake(buffer) );
}

void NodeHandshake::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	buffer.write(port);
	buffer.write( static_cast<uint64_t>(data.size()) );
	for ( auto &e : data )
		buffer.write(e);
}

const std::vector<CacheHandshake>& NodeHandshake::get_data() const {
	return data;
}

std::string NodeHandshake::to_string() const {
	return concat("NodeHandshake[port: ", port, "]");
}

template class CacheContent<NodeEntryStats>;
template class CacheContent<HandshakeEntry> ;

