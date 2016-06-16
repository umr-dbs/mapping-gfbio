/*
 * querymanager.cpp
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#include "cache/index/querymanager.h"
#include "cache/index/query_manager/default_query_manager.h"
#include "cache/index/query_manager/simple_query_manager.h"
#include "cache/index/query_manager/emkde_query_manager.h"
#include "cache/index/indexserver.h"
#include "cache/common.h"
#include "util/make_unique.h"

#include <algorithm>

//////////////////////////////////////////////////////////////////////
//
// LOCKS
//
//////////////////////////////////////////////////////////////////////



CacheLocks::Lock::Lock(CacheType type, const IndexCacheKey& key) :
	IndexCacheKey(key), type(type) {
}

bool CacheLocks::Lock::operator <(const Lock& l) const {
	return type < l.type || (type == l.type && IndexCacheKey::operator <(l));
}

bool CacheLocks::Lock::operator ==(const Lock& l) const {
	return type == l.type && IndexCacheKey::operator ==(l);
}


bool CacheLocks::is_locked(CacheType type, const IndexCacheKey& key) const {
	return is_locked( Lock(type,key) );
}

bool CacheLocks::is_locked(const Lock& lock) const {
	return locks.find(lock) != locks.end();
}

void CacheLocks::add_lock(const Lock& lock, uint64_t query_id) {
	auto it = locks.find(lock);
	if ( it != locks.end() ) {
		if ( !it->second.emplace(query_id).second )
			throw IllegalStateException(concat("Could not add lock ", lock.to_string(), " to query ", query_id));
	}
	else if ( !locks.emplace( lock, std::unordered_set<uint64_t>{query_id} ).second )
		throw IllegalStateException(concat("Could not add lock ", lock.to_string(), " to query ", query_id));
}

void CacheLocks::add_locks(const std::set<Lock>& locks, uint64_t query_id) {
	for ( auto &l : locks )
		add_lock(l,query_id);
}

void CacheLocks::remove_lock(const Lock& lock, uint64_t query_id) {
	auto it = locks.find(lock);
		if ( it != locks.end() ) {
			if ( it->second.erase(query_id) == 0 )
				throw IllegalStateException(concat("Lock: ", it->first.to_string(), " was not set for query: ", query_id));
			if ( it->second.empty() )
				locks.erase(it);
		}
		else
			throw ArgumentException(concat("No lock held for key: ", lock.to_string()) );
}

void CacheLocks::remove_locks(const std::set<Lock>& locks, uint64_t query_id) {
	for ( auto &l : locks )
			remove_lock(l,query_id);
}

std::unordered_set<uint64_t> CacheLocks::get_queries(const Lock& lock) const {
	return locks.at(lock);
}

void CacheLocks::move_lock(const Lock& from, const Lock& to, uint64_t query_id) {
	remove_lock(from, query_id);
	add_lock(to, query_id);
}

//
//
//

CacheLocks QueryManager::locks;

std::unique_ptr<QueryManager> QueryManager::by_name(IndexCacheManager& mgr, const std::map<uint32_t,std::shared_ptr<Node>> &nodes,
		const std::string& name) {
	std::string lcname;
	lcname.resize(name.size());
	std::transform(name.cbegin(), name.cend(), lcname.begin(), ::tolower);

	if ( name == "default" )
		return make_unique<DefaultQueryManager>(nodes,mgr);
	else if ( name == "dema" )
		return make_unique<DemaQueryManager>(nodes);
	else if ( name == "bema" )
		return make_unique<BemaQueryManager>(nodes);
	else if ( name == "emkde" )
		return make_unique<EMKDEQueryManager>(nodes);
	else throw ArgumentException(concat("Illegal scheduler name: ", name));
}

QueryManager::QueryManager(const std::map<uint32_t, std::shared_ptr<Node>> &nodes ) : nodes(nodes) {
}

void QueryManager::schedule_pending_jobs() {

	size_t num_workers = 0;
	for ( auto &kv : nodes ) {
		num_workers += kv.second->num_idle_workers();
	}

	auto it = pending_jobs.begin();
	while (  num_workers > 0 &&  it != pending_jobs.end()) {
		auto &q = *it->second;

		auto nids = q.get_target_nodes();
		uint64_t worker = 0;

		for ( auto nid = nids.begin(); worker == 0 && nid != nids.end(); nid++ ) {
			// Schedule on any node
			if ( (*nid) == 0 ) {
				for ( auto ni = nodes.begin(); ni != nodes.end() && worker == 0; ni++ ) {
					worker = ni->second->schedule_request(q.get_command(),q.get_request());
				}
			}
			else {
				auto &node = nodes.at(*nid);
				worker = node->schedule_request(q.get_command(),q.get_request());
			}
		}

		// Found a worker... Done!
		if ( worker > 0 ) {
			num_workers--;
			q.time_scheduled = CacheCommon::time_millis();
			Log::debug("Scheduled request: %s\non worker: %d", q.get_request().to_string().c_str(), worker);
			queries.emplace(worker, std::move(it->second));
			it = pending_jobs.erase(it);
		}
		// Suspend scheduling
		else
			++it;
	}
}

size_t QueryManager::close_worker(uint64_t worker_id) {
	auto it = queries.find(worker_id);
	if ( it == queries.end() )
		throw IllegalStateException(concat("No active query found for worker: ",worker_id));
	size_t res = it->second->get_clients().size();
	finished_queries.insert(std::move(*it));
	queries.erase(it);
	return res;
}

std::set<uint64_t> QueryManager::release_worker(uint64_t worker_id, uint32_t node_id) {
	auto it = finished_queries.find(worker_id);
	if ( it == finished_queries.end() )
		throw IllegalStateException(concat("No finished query found for worker: ",worker_id));

	auto &q = *(it->second);

	std::set<uint64_t> clients = q.get_clients();
	q.time_finished = CacheCommon::time_millis();

	// Tell on which node the queries were scheduled
	stats.scheduled(node_id, clients.size());

	for ( auto &tp : q.client_times ) {
		uint64_t wait = tp > q.time_scheduled ? 0 : q.time_scheduled - tp;
		stats.query_finished(wait,q.time_finished - std::max(tp,q.time_scheduled));
	}
	finished_queries.erase(it);
	return clients;
}

void QueryManager::worker_failed(uint64_t worker_id) {
	Log::info("Worker with id: %lu failed. Rescheduling jobs!", worker_id);
	auto fi = finished_queries.find(worker_id);
	if ( fi != finished_queries.end() ) {
		auto job = recreate_job(*fi->second);
		finished_queries.erase(fi);
		add_query(std::move(job));
		return;
	}

	fi = queries.find(worker_id);
	if ( fi != queries.end() ) {
		auto job = recreate_job(*fi->second);
		queries.erase(fi);
		add_query(std::move(job));
	}
}

void QueryManager::node_failed(uint32_t node_id) {
	Log::info("Node with id: %u failed. Rescheduling jobs!", node_id);
	auto iter = pending_jobs.begin();

	while ( iter != pending_jobs.end() ) {
		if ( iter->second->is_affected_by_node(node_id) ) {
			auto nj = recreate_job(*iter->second);
			iter->second = std::move(nj);
		}
		iter++;
	}
}

void QueryManager::handle_client_abort(uint64_t client_id) {
	auto iter = pending_jobs.begin();
	while ( iter != pending_jobs.end() ) {
		auto &jd = *iter->second;
		if ( jd.remove_client(client_id) && !jd.has_clients() ) {
			Log::info("Cancelled request for client: %ld", client_id);
			pending_jobs.erase(iter);
			return;
		}
		iter++;
	}
}

SystemStats& QueryManager::get_stats() {
	return stats;
}

void QueryManager::reset_stats() {
	stats.reset();
}

bool QueryManager::is_locked( CacheType type, const IndexCacheKey& key) const {
	return locks.is_locked(type,key);
}

bool QueryManager::process_move(CacheType type, const IndexCacheKey& from, const IndexCacheKey& to) {
	CacheLocks::Lock lfrom(type,from);
	CacheLocks::Lock lto(type,to);
	if ( locks.is_locked(lfrom) ) {
		auto qids = locks.get_queries(lfrom);
		auto qiter = qids.begin();
		while ( qiter != qids.end() ) {
			try {
				pending_jobs.at(*qiter)->entry_moved(lfrom,lto,nodes);
				qiter = qids.erase(qiter);
			} catch ( const std::out_of_range &oor ) {
				qiter++;
			}
		}
		return qids.empty();
	}
	else
		return true;
}

void QueryManager::add_query(std::unique_ptr<PendingQuery> query) {
	pending_jobs.emplace(query->id, std::move(query));
}

//
// Jobs
//

uint64_t RunningQuery::next_id = 1;

RunningQuery::RunningQuery( std::set<CacheLocks::Lock> &&locks ) :
	id(next_id++), locks(std::move(locks)), time_created(CacheCommon::time_millis()), time_scheduled(0), time_finished(0) {
	QueryManager::locks.add_locks(this->locks,id);
}

RunningQuery::~RunningQuery() {
	QueryManager::locks.remove_locks(this->locks,id);
}

void RunningQuery::add_lock(const CacheLocks::Lock& lock) {
	QueryManager::locks.add_lock(lock,id);
	locks.emplace(lock);
}

void RunningQuery::add_locks(const std::set<CacheLocks::Lock>& locks) {
	QueryManager::locks.add_locks(locks,id);
	this->locks.insert(locks.begin(),locks.end());
}


bool RunningQuery::satisfies( const BaseRequest& req) const {
	const BaseRequest &my_req = get_request();
	if ( req.type == my_req.type &&
		 req.semantic_id == my_req.semantic_id &&
		 req.query.restype == my_req.query.restype &&
		 req.query.timetype == my_req.query.timetype &&
		 my_req.query.SpatialReference::contains(req.query) &&
		 my_req.query.TemporalReference::contains(req.query) ) {

		if ( my_req.query.restype == QueryResolution::Type::NONE)
			return true;
		else if (my_req.query.restype == QueryResolution::Type::PIXELS) {
			// Check resolution
			double my_xres = (my_req.query.x2 - my_req.query.x1) / my_req.query.xres;
			double my_yres = (my_req.query.y2 - my_req.query.y1) / my_req.query.yres;

			double q_xres = (req.query.x2 - req.query.x1) / req.query.xres;
			double q_yres = (req.query.y2 - req.query.y1) / req.query.yres;

			return std::abs(1.0 - my_xres / q_xres) < 0.01 && std::abs(1.0 - my_yres / q_yres) < 0.01;
		}
		else
			throw ArgumentException("Unknown QueryResolution::Type in QueryRectangle");
	}
	return false;
}

void RunningQuery::add_client(uint64_t client) {
	clients.insert(client);
	client_times.push_back(CacheCommon::time_millis());
}

void RunningQuery::add_clients(const std::set<uint64_t>& clients) {
	this->clients.insert(clients.begin(),clients.end());
	for ( size_t i = 0; i < clients.size(); i++ )
		client_times.push_back(CacheCommon::time_millis());
}

const std::set<uint64_t>& RunningQuery::get_clients() const {
	return clients;
}

bool RunningQuery::remove_client(uint64_t client_id) {
	return clients.erase(client_id) > 0;
}

bool RunningQuery::has_clients() const {
	return !clients.empty();
}

PendingQuery::PendingQuery( std::set<CacheLocks::Lock> &&locks ) :
	RunningQuery(std::move(locks)) {
}

void PendingQuery::entry_moved(const CacheLocks::Lock& from, const CacheLocks::Lock& to, const std::map<uint32_t, std::shared_ptr<Node>> &nmap) {
	QueryManager::locks.move_lock(from,to,id);
	locks.erase(from);
	locks.insert(to);
	replace_reference(from,to,nmap);
}
