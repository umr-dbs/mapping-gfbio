/*
 * querymanager.cpp
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#include "cache/index/querymanager.h"
#include "cache/index/indexserver.h"
#include "cache/common.h"
#include "util/make_unique.h"

#include <algorithm>

CacheLocks::Lock::Lock(CacheType type, const IndexCacheKey& key) :
	IndexCacheKey(key), type(type) {
}

bool CacheLocks::Lock::operator <(const Lock& l) const {
	return (type <  l.type) ||
		   (type == l.type && entry_id <  l.entry_id) ||
		   (type == l.type && entry_id == l.entry_id && node_id <  l.node_id) ||
		   (type == l.type && entry_id == l.entry_id && node_id == l.node_id && semantic_id < l.semantic_id);
}

bool CacheLocks::Lock::operator ==(const Lock& l) const {
	return type == l.type &&
		   entry_id == l.entry_id &&
		   node_id == l.node_id &&
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

QueryManager::QueryManager(IndexCaches &caches, const std::map<uint32_t, std::shared_ptr<Node>> &nodes) :
	caches(caches), nodes(nodes) {
}

void QueryManager::add_request(uint64_t client_id, const BaseRequest &req ) {
	ExecTimer t("QueryManager.add_request");
	// Check if running jobs satisfy the given query
	for (auto &qi : queries) {
		if (qi.second->satisfies(req)) {
			qi.second->add_client(client_id);
			return;
		}
	}

	// Check if pending jobs satisfy the given query
	for (auto &j : pending_jobs) {
		if (j->satisfies(req)) {
			j->add_client(client_id);
			return;
		}
	}

	// Perform a cache-query
	auto &cache = caches.get_cache(req.type);
	auto res = cache.query(req.semantic_id, req.query);
	Log::debug("QueryResult: %s", res.to_string().c_str());

	//  No result --> Check if a pending query may be extended by the given query
	if ( res.keys.empty() ) {
		for (auto &j : pending_jobs) {
			if (j->extend(req)) {
				j->add_client(client_id);
				return;
			}
		}
	}
	// Create a new job
	auto job = create_job(req,cache,res);
	job->add_client(client_id);
	pending_jobs.push_back(std::move(job));
}

void QueryManager::schedule_pending_jobs(
	const std::map<uint64_t, std::unique_ptr<WorkerConnection> > &worker_connections) {

	auto it = pending_jobs.begin();
	while (it != pending_jobs.end()) {
		uint64_t con_id = (*it)->schedule(worker_connections);
		if (con_id != 0) {
			Log::info("Scheduled request: %s\non worker: %d", (*it)->get_request().to_string().c_str(), con_id);
			queries.emplace(con_id, std::move(*it));
			it = pending_jobs.erase(it);
		}
		else
			++it;
	}
}

size_t QueryManager::close_worker(uint64_t worker_id) {
	auto it = queries.find(worker_id);
	size_t res = it->second->get_clients().size();
	finished_queries.insert(std::move(*it));
	queries.erase(it);
	return res;
}

std::set<uint64_t> QueryManager::release_worker(uint64_t worker_id) {
	auto it = finished_queries.find(worker_id);
	std::set<uint64_t> clients = it->second->get_clients();
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

void QueryManager::process_worker_query(WorkerConnection& con) {
	auto &req = con.get_query();
	auto &query = *queries.at(con.id);
	auto &cache = caches.get_cache(req.type);
	auto res = cache.query(req.semantic_id, req.query);
	Log::debug("QueryResult: %s", res.to_string().c_str());

	// Full single hit
	if (res.keys.size() == 1 && !res.has_remainder()) {
		Log::debug("Full HIT. Sending reference.");
		IndexCacheKey key(req.semantic_id, res.keys.at(0));
		auto ref = cache.get(key);
		auto node = nodes.at(ref->node_id);
		CacheRef cr(node->host, node->port, ref->entry_id);
		// Apply lock
		query.add_lock( CacheLocks::Lock(req.type,key) );
		con.send_hit(cr);
	}
	// Puzzle
	else if (res.has_hit() ) {
		Log::debug("Partial HIT. Sending puzzle-request, coverage: %f");
		std::vector<CacheRef> entries;
		for (auto id : res.keys) {
			IndexCacheKey key(req.semantic_id, id);
			auto ref = cache.get(key);
			auto &node = nodes.at(ref->node_id);
			query.add_lock(CacheLocks::Lock(req.type,key));
			entries.push_back(CacheRef(node->host, node->port, ref->entry_id));
		}
		PuzzleRequest pr( req.type, req.semantic_id, req.query, res.remainder, entries);
		con.send_partial_hit(pr);
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		con.send_miss();
	}
}



//
// PRIVATE
//

std::unique_ptr<PendingQuery> QueryManager::create_job( const BaseRequest &req, const IndexCache &cache, const CacheQueryResult<std::pair<uint32_t,uint64_t>>& res) {
	ExecTimer t("QueryManager.create_job");

	// Full single hit
	if (res.keys.size() == 1 && !res.has_remainder()) {
		Log::debug("Full HIT. Sending reference.");
		IndexCacheKey key(req.semantic_id, res.keys.at(0));
		auto ref = cache.get(key);
		DeliveryRequest dr(
				req.type,
				req.semantic_id,
				res.covered,
				ref->entry_id);
		return make_unique<DeliverJob>(std::move(dr), key);
	}
	// Puzzle
	else if (res.has_hit()) {
		Log::debug("Partial HIT. Sending puzzle-request.");
		std::vector<uint32_t> node_ids;
		std::vector<IndexCacheKey> keys;
		std::vector<CacheRef> entries;
		for (auto id : res.keys) {
			IndexCacheKey key(req.semantic_id, id);
			auto ref = cache.get(key);
			auto &node = nodes.at(ref->node_id);
			keys.push_back(key);
			node_ids.push_back(ref->node_id);
			entries.push_back(CacheRef(node->host, node->port, ref->entry_id));
		}
		PuzzleRequest pr(
				req.type,
				req.semantic_id,
				res.covered,
				res.remainder, entries);
		return make_unique<PuzzleJob>(std::move(pr), std::move(keys));
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		return make_unique<CreateJob>(BaseRequest(req), nodes, cache);
	}
}

void QueryManager::node_failed(uint32_t node_id) {
	auto iter = pending_jobs.begin();

	while ( iter != pending_jobs.end() ) {
		if ( (*iter)->is_affected_by_node(node_id) ) {
			auto nj = recreate_job(**iter);
			iter = pending_jobs.erase(iter);
			iter = pending_jobs.insert(iter,std::move(nj));
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

std::unique_ptr<PendingQuery> QueryManager::recreate_job(const RunningQuery& query) {
	auto &req = query.get_request();
	auto &cache = caches.get_cache(req.type);
	auto res = cache.query(req.semantic_id, req.query);
	auto job = create_job( req, cache, res );
	job->add_clients( query.get_clients() );
	return job;
}

bool QueryManager::is_locked( CacheType type, const IndexCacheKey& key) {
	return locks.is_locked(type,key);
}

//
// Jobs
//

RunningQuery::RunningQuery( std::vector<CacheLocks::Lock> &&locks ) :
	locks(std::move(locks)) {
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
}

void RunningQuery::add_clients(const std::set<uint64_t>& clients) {
	this->clients.insert(clients.begin(),clients.end());
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

CreateJob::CreateJob( BaseRequest&& request,
					 const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const IndexCache &cache) :
	PendingQuery(), request(request),
	orig_query(this->request.query),
	orig_area( (this->request.query.x2 - this->request.query.x1) * (this->request.query.y2 - this->request.query.y1)),
	nodes(nodes), cache(cache) {
}

bool CreateJob::extend(const BaseRequest& req) {
	if ( req.type == request.type &&
		 req.semantic_id == request.semantic_id &&
		 orig_query.TemporalReference::contains(req.query) &&
		 orig_query.restype == req.query.restype) {

		double nx1, nx2, ny1, ny2, narea;

		nx1 = std::min(request.query.x1, req.query.x1);
		ny1 = std::min(request.query.y1, req.query.y1);
		nx2 = std::max(request.query.x2, req.query.x2);
		ny2 = std::max(request.query.y2, req.query.y2);

		narea = (nx2 - nx1) * (ny2 - ny1);

		if (orig_query.restype == QueryResolution::Type::NONE && narea / orig_area <= 4.01) {
			SpatialReference sref(orig_query.epsg, nx1, ny1, nx2, ny2);
			request.query = QueryRectangle(sref, orig_query, orig_query);
			return true;
		}
		else if (orig_query.restype == QueryResolution::Type::PIXELS && narea / orig_area <= 4.01) {
			// Check resolution
			double my_xres = (orig_query.x2 - orig_query.x1) / orig_query.xres;
			double my_yres = (orig_query.y2 - orig_query.y1) / orig_query.yres;

			double q_xres = (req.query.x2 - req.query.x1) / req.query.xres;
			double q_yres = (req.query.y2 - req.query.y1) / req.query.yres;

			if (std::abs(1.0 - my_xres / q_xres) < 0.01 && std::abs(1.0 - my_yres / q_yres) < 0.01) {

				uint32_t nxres = std::ceil(
					orig_query.xres * ((nx2 - nx1) / (orig_query.x2 - orig_query.x1)));
				uint32_t nyres = std::ceil(
					orig_query.yres * ((ny2 - ny1) / (orig_query.y2 - orig_query.y1)));

				SpatialReference sref(orig_query.epsg, nx1, ny1, nx2, ny2);
				request.query = QueryRectangle(sref, orig_query, QueryResolution::pixels(nxres, nyres));
				return true;
			}
		}
	}
	return false;
}

uint64_t CreateJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	// Do not schedule if we have no nodes
	if ( nodes.empty() )
		return 0;

	uint32_t node_id = cache.get_node_for_job(request,nodes);
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.node->id == node_id && con.get_state() == WorkerState::IDLE) {
			con.process_request(WorkerConnection::CMD_CREATE, request);
			return con.id;
		}
	}
	// Fallback
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.get_state() == WorkerState::IDLE) {
			con.process_request(WorkerConnection::CMD_CREATE, request);
			return con.id;
		}
	}


	return 0;
}

bool CreateJob::is_affected_by_node(uint32_t node_id) {
	(void) node_id;
	return false;
}

const BaseRequest& CreateJob::get_request() const {
	return request;
}

//
// DELIVCER JOB
//

DeliverJob::DeliverJob(DeliveryRequest&& request, const IndexCacheKey &key) :
	PendingQuery(std::vector<CacheLocks::Lock>{CacheLocks::Lock(request.type,key)}),
	request(request),
	node(key.node_id) {
}

uint64_t DeliverJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.node->id == node && con.get_state() == WorkerState::IDLE) {
			con.process_request(WorkerConnection::CMD_DELIVER, request);
			return con.id;
		}
	}
	return 0;
}

bool DeliverJob::is_affected_by_node(uint32_t node_id) {
	return node_id == node;
}

bool DeliverJob::extend(const BaseRequest&) {
	return false;
}

const BaseRequest& DeliverJob::get_request() const {
	return request;
}

//
// PUZZLE JOB
//

PuzzleJob::PuzzleJob(PuzzleRequest&& request, const std::vector<IndexCacheKey> &keys) :
	PendingQuery(),
	request(std::move(request)) {
	for ( auto &k : keys ) {
		nodes.insert(k.node_id);
		add_lock( CacheLocks::Lock(request.type,k) );
	}
}

uint64_t PuzzleJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &node : nodes) {
		for (auto &e : connections) {
			auto &con = *e.second;
			if (!con.is_faulty() && con.node->id == node
				&& con.get_state() == WorkerState::IDLE) {
				con.process_request(WorkerConnection::CMD_PUZZLE, request);
				return con.id;
			}
		}
	}
	return 0;
}

bool PuzzleJob::is_affected_by_node(uint32_t node_id) {
	return nodes.find(node_id) != nodes.end();
}


bool PuzzleJob::extend(const BaseRequest&) {
	return false;
}

const BaseRequest& PuzzleJob::get_request() const {
	return request;
}

