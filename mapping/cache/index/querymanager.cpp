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
	return (type <  l.type) ||
		   (type == l.type && id.first <  l.id.first) ||
		   (type == l.type && id.first == l.id.first && id.second <  l.id.second) ||
		   (type == l.type && id.first == l.id.first && id.second == l.id.second && semantic_id < l.semantic_id);
}

bool CacheLocks::Lock::operator ==(const Lock& l) const {
	return type == l.type &&
		   id == l.id &&
		   semantic_id == l.semantic_id;
}


bool CacheLocks::is_locked(CacheType type, const IndexCacheKey& key) const {
	return is_locked( Lock(type,key) );
}

bool CacheLocks::is_locked(const Lock& lock) const {
	return locks.find(lock) != locks.end();
}

void CacheLocks::add_lock(const Lock& lock) {
	auto it = locks.find(lock);
	if ( it != locks.end() )
		it->second++;
	else if ( !locks.emplace( lock, 1 ).second )
		throw IllegalStateException("Locking failed!");
}

void CacheLocks::add_locks(const std::vector<Lock>& locks) {
	for ( auto &l : locks )
		add_lock(l);
}

void CacheLocks::remove_lock(const Lock& lock) {
	auto it = locks.find(lock);
		if ( it != locks.end() ) {
			if ( it->second == 1 )
				locks.erase(it);
			else if ( it->second > 1 )
				it->second--;
			else
				throw IllegalStateException("Illegal state on locks!");
		}
		else
			throw ArgumentException(concat("No lock held for key: ", lock.to_string()) );
}

void CacheLocks::remove_locks(const std::vector<Lock>& locks) {
	for ( auto &l : locks )
			remove_lock(l);
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
		return make_unique<DemaQueryManager>(nodes,mgr);
	else if ( name == "bema" )
			return make_unique<BemaQueryManager>(nodes,mgr);
	else if ( name == "emkde" )
				return make_unique<EMKDEQueryManager>(nodes,mgr);
	else if ( name == "hybrid" )
					return make_unique<HybridQueryManager>(nodes,mgr);
	else throw ArgumentException(concat("Illegal scheduler name: ", name));
}

QueryManager::QueryManager(const std::map<uint32_t, std::shared_ptr<Node>> &nodes, IndexCacheManager &caches) : nodes(nodes), caches(caches) {
}

void QueryManager::schedule_pending_jobs(
	const std::map<uint64_t, std::unique_ptr<WorkerConnection> > &worker_connections) {

	auto it = pending_jobs.begin();
	while (it != pending_jobs.end()) {
		uint64_t con_id = (*it)->schedule(worker_connections);
		if (con_id != 0) {
			(*it)->time_scheduled = CacheCommon::time_millis();
			Log::debug("Scheduled request: %s\non worker: %d", (*it)->get_request().to_string().c_str(), con_id);
			queries.emplace(con_id, std::move(*it));
			it = pending_jobs.erase(it);
		}
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
	auto fi = finished_queries.find(worker_id);
	if ( fi != finished_queries.end() ) {
		auto job = recreate_job(*fi->second);
		finished_queries.erase(fi);
		pending_jobs.push_back(std::move(job));
		return;
	}

	fi = queries.find(worker_id);
	if ( fi != queries.end() ) {
		auto job = recreate_job(*fi->second);
		queries.erase(fi);
		pending_jobs.push_back(std::move(job));
	}
}

void QueryManager::node_failed(uint32_t node_id) {
	auto iter = pending_jobs.begin();

	while ( iter != pending_jobs.end() ) {
		if ( (*iter)->is_affected_by_node(node_id) ) {
			auto nj = recreate_job(**iter);
			*iter = std::move(nj);
		}
		iter++;
	}
}

void QueryManager::handle_client_abort(uint64_t client_id) {
	auto iter = pending_jobs.begin();
	while ( iter != pending_jobs.end() ) {
		auto &jd = **iter;
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

//
// Jobs
//

RunningQuery::RunningQuery( std::vector<CacheLocks::Lock> &&locks ) :
	locks(std::move(locks)), time_created(CacheCommon::time_millis()), time_scheduled(0), time_finished(0) {
	QueryManager::locks.add_locks(this->locks);
}

RunningQuery::~RunningQuery() {
	QueryManager::locks.remove_locks(this->locks);
}

void RunningQuery::add_lock(const CacheLocks::Lock& lock) {
	QueryManager::locks.add_lock(lock);
	locks.push_back(lock);
}

void RunningQuery::add_locks(const std::vector<CacheLocks::Lock>& locks) {
	QueryManager::locks.add_locks(locks);
	this->locks.insert(this->locks.end(),locks.begin(),locks.end());
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

PendingQuery::PendingQuery( std::vector<CacheLocks::Lock> &&locks ) :
	RunningQuery(std::move(locks)) {
}

