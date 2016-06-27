/*
 * cache_stats.cpp
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#include "cache/priv/cache_stats.h"
#include "util/concat.h"

#include <limits>


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
	single_remote_hits(0), multi_remote_hits(0), multi_remote_partials(0), misses(0), result_bytes(0), lost_puts(0), queries(0), ratios(0) {
}

QueryStats::QueryStats(BinaryReadBuffer& buffer) :
	single_local_hits(buffer.read<uint32_t>()),
	multi_local_hits(buffer.read<uint32_t>()),
	multi_local_partials(buffer.read<uint32_t>()),
	single_remote_hits(buffer.read<uint32_t>()),
	multi_remote_hits(buffer.read<uint32_t>()),
	multi_remote_partials(buffer.read<uint32_t>()),
	misses(buffer.read<uint32_t>()),
	result_bytes(buffer.read<uint64_t>()),
	lost_puts(buffer.read<uint64_t>()),
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
	res.result_bytes += stats.result_bytes;
	res.lost_puts += stats.lost_puts;
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
	result_bytes += stats.result_bytes;
	lost_puts += stats.lost_puts;
	queries += stats.queries;
	ratios += stats.ratios;
	return *this;
}

void QueryStats::serialize(BinaryWriteBuffer& buffer, bool) const {
	buffer << single_local_hits << multi_local_hits << multi_local_partials;
	buffer << single_remote_hits << multi_remote_hits << multi_remote_partials;
	buffer << misses << result_bytes << lost_puts << queries << ratios;
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
	result_bytes = 0;
	lost_puts = 0;
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
	ss << "  hit-ratio         : " << (ratios / queries) << std::endl;
	ss << "  cache-queries     : " << queries << std::endl;
	ss << "  result-bytes      : " << result_bytes << std::endl;
	ss << "  lost puts         : " << lost_puts;
	return ss.str();
}

///////////////////////////////////////////////////////////
//
// SYSTEM-STATS
//
///////////////////////////////////////////////////////////

SystemStats::SystemStats() : QueryStats(),
	queries_issued(0),
	queries_scheduled(0), query_counter(0), reorg_cycles(0),

	max_reorg_time(0),
	min_reorg_time(std::numeric_limits<double>::infinity()),
	avg_reorg_time(0),

	max_wait_time(0),
	min_wait_time(std::numeric_limits<double>::infinity()),
	avg_wait_time(0),

	max_exec_time(0),
	min_exec_time(std::numeric_limits<double>::infinity()),
	avg_exec_time(0),

	max_time(0),
	min_time(std::numeric_limits<double>::infinity()),
	avg_time(0) {
}

SystemStats SystemStats::operator +(const SystemStats& stats) const {
	SystemStats res;
	res += *this;
	res += stats;
	return res;
}

SystemStats& SystemStats::operator +=(const SystemStats& stats) {
	QueryStats::operator +=(stats);

	if ( reorg_cycles + stats.reorg_cycles > 0 ) {
		max_reorg_time = std::max(max_reorg_time,stats.max_reorg_time);
		min_reorg_time = std::min(min_reorg_time,stats.min_reorg_time);
		avg_reorg_time = ((avg_reorg_time*reorg_cycles) + (stats.avg_reorg_time*stats.reorg_cycles)) / (reorg_cycles + stats.reorg_cycles);
	}

	if ( query_counter + stats.query_counter > 0 ) {
		max_wait_time = std::max(max_wait_time,stats.max_wait_time);
		min_wait_time = std::min(min_wait_time,stats.min_wait_time);
		avg_wait_time = ((avg_wait_time*query_counter) + (stats.avg_wait_time*stats.query_counter)) / (query_counter + stats.query_counter);

		max_exec_time = std::max(max_exec_time,stats.max_exec_time);
		min_exec_time = std::min(min_exec_time,stats.min_exec_time);
		avg_exec_time = ((avg_exec_time*query_counter) + (stats.avg_exec_time*stats.query_counter)) / (query_counter + stats.query_counter);

		max_time = std::max(max_time,stats.max_time);
		min_time = std::min(min_time,stats.min_time);
		avg_time = ((avg_time*query_counter) + (stats.avg_time*stats.query_counter)) / (query_counter + stats.query_counter);
	}

	queries_issued += stats.queries_issued;
	queries_scheduled += stats.queries_scheduled;
	query_counter += stats.query_counter;
	reorg_cycles += stats.reorg_cycles;

	for ( auto &p : stats.node_to_queries ) {
		auto i = node_to_queries.find(p.first);
		if ( i == node_to_queries.end() )
			node_to_queries.emplace(p.first,p.second);
		else
			i->second += p.second;
	}
	return *this;
}

SystemStats& SystemStats::operator +=(const QueryStats& stats) {
	QueryStats::operator +=(stats);
	return *this;
}

void SystemStats::reset() {
	QueryStats::reset();
	queries_issued = 0;
	queries_scheduled = 0;
	query_counter = 0;
	reorg_cycles = 0;

	max_reorg_time = 0;
	min_reorg_time = std::numeric_limits<double>::infinity();
	avg_reorg_time = 0;

	max_wait_time = 0;
	min_wait_time = std::numeric_limits<double>::infinity();
	avg_wait_time = 0;

	max_exec_time = 0;
	min_exec_time = std::numeric_limits<double>::infinity();
	avg_exec_time = 0;

	max_time = 0;
	min_time = std::numeric_limits<double>::infinity();
	avg_time = 0;

  node_to_queries.clear();
}

std::string SystemStats::to_string() const {
	double q_ratio = (double) queries_scheduled / (double) queries_issued;

	std::ostringstream ss;
	ss << "Index-Stats:" << std::endl;
	ss << "  single hits               : " << single_local_hits << std::endl;
	ss << "  single remote hits        : " << single_remote_hits << std::endl;
	ss << "  puzzle single node        : " << multi_local_hits << std::endl;
	ss << "  puzzle multiple nodes     : " << multi_remote_hits << std::endl;
	ss << "  partial single node       : " << multi_local_partials << std::endl;
	ss << "  partial multiple nodes    : " << multi_remote_partials << std::endl;
	ss << "  misses                    : " << misses << std::endl;
	ss << "  result-bytes              : " << result_bytes << std::endl;
	ss << "  lost puts                 : " << lost_puts << std::endl;
	ss << "  hit ratio                 : " << get_hit_ratio() << std::endl;
	ss << "  cache-queries             : " << queries << std::endl;
	ss << "  requests received         : " << queries_issued << std::endl;
	ss << "  requests scheduled        : " << queries_scheduled << std::endl;
	ss << "  reorg cycles              : " << reorg_cycles << std::endl;

	ss << "  max reorg duration        : " << max_reorg_time << std::endl;
	ss << "  min reorg duration        : " << min_reorg_time << std::endl;
	ss << "  avg reorg duration        : " << avg_reorg_time << std::endl;

	ss << "  max query wait-time       : " << max_wait_time << std::endl;
	ss << "  min query wait-time       : " << min_wait_time << std::endl;
	ss << "  avg query wait-time       : " << avg_wait_time << std::endl;
	ss << "  avg query wait-time (norm): " << (avg_wait_time*q_ratio) << std::endl;

	ss << "  max query exec-time       : " << max_exec_time << std::endl;
	ss << "  min query exec-time       : " << min_exec_time << std::endl;
	ss << "  avg query exec-time       : " << avg_exec_time << std::endl;
	ss << "  avg query exec-time (norm): " << (avg_exec_time*q_ratio) << std::endl;

	ss << "  max query time            : " << max_time << std::endl;
	ss << "  min query time            : " << min_time << std::endl;
	ss << "  avg query time            : " << avg_time << std::endl;
	ss << "  avg query time (norm)     : " << (avg_time*q_ratio) << std::endl;

	ss << "  distrib (NodeId:#Queries): ";
	for ( auto &p : node_to_queries )
		ss << "(" << p.first << ": " << p.second << "), ";
	return ss.str();
}

uint32_t SystemStats::get_queries_scheduled() {
	return queries_scheduled;
}

void SystemStats::scheduled(uint32_t node_id) {
	queries_scheduled++;
	auto it = node_to_queries.find(node_id);
	if ( it == node_to_queries.end() )
		node_to_queries.emplace(node_id, 1);
	else
		it->second++;
}


void SystemStats::query_finished(uint64_t wait_time, uint64_t exec_time ) {
	double w = (double) wait_time;
	double e = (double) exec_time;

	avg_exec_time = (avg_exec_time*query_counter + exec_time) / (query_counter+1);
	avg_wait_time = (avg_wait_time*query_counter + wait_time) / (query_counter+1);
	avg_time = avg_exec_time + avg_wait_time;

	min_wait_time = std::min(w, min_wait_time);
	min_exec_time = std::min(e, min_exec_time);
	min_time = std::min(w+e, min_time);

	max_wait_time = std::max(w, max_wait_time);
	max_exec_time = std::max(e, max_exec_time);
	max_time = std::max(w+e, max_time);

	query_counter++;
}

void SystemStats::add_reorg_cycle(uint64_t duration) {
	avg_reorg_time = (avg_reorg_time*reorg_cycles + duration) / (reorg_cycles+1);
	min_reorg_time = std::min((double)duration, min_reorg_time);
	max_reorg_time = std::max((double)duration, max_reorg_time);
	reorg_cycles++;
}

SystemStats::SystemStats(BinaryReadBuffer& buffer) : QueryStats(buffer),
		queries_issued(buffer.read<uint32_t>()),
		queries_scheduled(buffer.read<uint32_t>()),
		query_counter(buffer.read<uint32_t>()),
		reorg_cycles(buffer.read<uint32_t>()),

		max_reorg_time(buffer.read<double>()),
		min_reorg_time(buffer.read<double>()),
		avg_reorg_time(buffer.read<double>()),

		max_wait_time(buffer.read<double>()),
		min_wait_time(buffer.read<double>()),
		avg_wait_time(buffer.read<double>()),

		max_exec_time(buffer.read<double>()),
		min_exec_time(buffer.read<double>()),
		avg_exec_time(buffer.read<double>()),

		max_time(buffer.read<double>()),
		min_time(buffer.read<double>()),
		avg_time(buffer.read<double>()) {

	uint64_t map_size = buffer.read<uint64_t>();

	for ( size_t i = 0; i < map_size; i++ ) {
		uint32_t k = buffer.read<uint32_t>();
		uint64_t v = buffer.read<uint64_t>();
		node_to_queries.emplace(k,v);
	}
}

void SystemStats::serialize(BinaryWriteBuffer& buffer,
		bool is_persistent_memory) const {
	QueryStats::serialize(buffer,is_persistent_memory);
	buffer << queries_issued << queries_scheduled << query_counter << reorg_cycles;

	buffer << max_reorg_time << min_reorg_time << avg_reorg_time ;
	buffer << max_wait_time << min_wait_time << avg_wait_time ;
	buffer << max_exec_time << min_exec_time << avg_exec_time;
	buffer << max_time << min_time << avg_time;

	uint64_t s = node_to_queries.size();
	buffer << s;
	for ( auto &p : node_to_queries ) {
		buffer << p.first << p.second;
	}
}

void SystemStats::issued() {
	queries_issued++;
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
		e.serialize(buffer, is_persistent_memory);
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
	(void) is_persistent_memory;
	buffer.write(port);
	buffer.write( static_cast<uint64_t>(data.size()) );
	for ( auto &e : data )
		e.serialize(buffer, is_persistent_memory);
}

const std::vector<CacheHandshake>& NodeHandshake::get_data() const {
	return data;
}

std::string NodeHandshake::to_string() const {
	return concat("NodeHandshake[port: ", port, "]");
}

template class CacheContent<NodeEntryStats>;
template class CacheContent<HandshakeEntry> ;

