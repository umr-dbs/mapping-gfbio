/*
 * querymanager.cpp
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "cache/index/query_manager/default_query_manager.h"
#include "cache/common.h"
#include "util/make_unique.h"

#include <algorithm>

DefaultQueryManager::DefaultQueryManager(const std::map<uint32_t, std::shared_ptr<Node>> &nodes, IndexCacheManager &caches, bool enable_batching) :
	QueryManager(nodes), caches(caches), enable_batching(enable_batching) {
}

bool DefaultQueryManager::use_reorg() const {
	return true;
}

void DefaultQueryManager::add_request(uint64_t client_id, const BaseRequest &req ) {
	stats.issued();
	TIME_EXEC("QueryManager.add_request");

	if ( enable_batching ) {
		// Check if running jobs satisfy the given query
		for (auto &qi : queries) {
			if (qi.second->satisfies(req)) {
				qi.second->add_client(client_id);
				return;
			}
		}

		// Check if pending jobs satisfy the given query
		for (auto &j : pending_jobs) {
			if (j.second->satisfies(req)) {
				j.second->add_client(client_id);
				return;
			}
		}

		// Perform a cache-query
		auto &cache = caches.get_cache(req.type);
		auto res = cache.query(req.semantic_id, req.query);
		stats.add_query(res.hit_ratio);
		Log::debug("QueryResult: %s", res.to_string().c_str());

		//  No result --> Check if a pending query may be extended by the given query
		if ( res.items.empty() ) {
			for (auto &j : pending_jobs) {
				if (j.second->extend(req)) {
					j.second->add_client(client_id);
					return;
				}
			}
		}
		// Create a new job
		auto job = create_job(req,res);
		job->add_client(client_id);
		add_query(std::move(job));
	}
	else {
		// Perform a cache-query
		auto &cache = caches.get_cache(req.type);
		auto res = cache.query(req.semantic_id, req.query);
		stats.add_query(res.hit_ratio);
		Log::debug("QueryResult: %s", res.to_string().c_str());
		auto job = create_job(req,res);
		job->add_client(client_id);
		add_query(std::move(job));
	}
}

void DefaultQueryManager::process_worker_query(WorkerConnection& con) {
	auto &req = con.get_query();
	try {
		queries.at(con.id);
		auto &cache = caches.get_cache(req.type);
		auto res = cache.query(req.semantic_id, req.query);
		Log::debug("QueryResult: %s", res.to_string().c_str());

		stats.add_query(res.hit_ratio);

		// Full single hit
		if (res.items.size() == 1 && !res.has_remainder()) {
			Log::debug("Full HIT. Sending reference.");
			IndexCacheKey key(req.semantic_id, res.items.front()->id);
			auto node = nodes.at(key.get_node_id());
			CacheRef cr(node->host, node->port, key.get_entry_id(),res.items.front()->bounds);
			// Apply lock
			con.send_hit(cr);
		}
		// Puzzle
		else if (res.has_hit() ) {
			Log::debug("Partial HIT. Sending puzzle-request, coverage: %f");
			std::vector<CacheRef> entries;
			for (auto &e : res.items) {
				auto &node = nodes.at(e->id.first);
				entries.push_back(CacheRef(node->host, node->port, e->id.second, e->bounds));
			}
			PuzzleRequest pr( req.type, req.semantic_id, req.query, std::move(res.remainder), std::move(entries) );
			con.send_partial_hit(pr);
		}
		// Full miss
		else {
			Log::debug("Full MISS.");
			con.send_miss();
		}
	} catch ( const std::out_of_range &oor ) {
		std::ostringstream aqs;
		for ( auto &p : queries ) {
			aqs << p.first << ",";
		}
		std::ostringstream fqs;
		for ( auto &p : finished_queries ) {
			fqs << p.first << ",";
		}

		std::ostringstream ns;
		for ( auto &p : nodes) {
			ns << p.second->to_string() << std::endl;
		}
		Log::error("No active query found for worker-query. WorkerID: %ul. Traceback:\nActive queries: %s\nFinished queries: %s\nNodes:\n%s", con.id, aqs.str().c_str(), fqs.str().c_str(), ns.str().c_str());
		throw IllegalStateException(concat("Worker ", con.id, " issued query w/o active query"));
	}
}



//
// PRIVATE
//

std::unique_ptr<PendingQuery> DefaultQueryManager::create_job( const BaseRequest &req, const CacheQueryResult<IndexCacheEntry>& res) {
	TIME_EXEC("DefaultQueryManager.create_job");

	// Full single hit
	if (res.items.size() == 1 && !res.has_remainder()) {
		stats.single_local_hits++;
		Log::debug("Full HIT. Sending reference.");
		IndexCacheKey key(req.semantic_id, res.items.front()->id);
		DeliveryRequest dr(
				req.type,
				req.semantic_id,
				res.covered,
				key.get_entry_id());
		return make_unique<DeliverJob>(std::move(dr), key);
	}
	// Puzzle
	else if (res.has_hit()) {
		Log::debug("Partial HIT. Sending puzzle-request.");
		std::set<uint32_t> node_ids;
		std::vector<IndexCacheKey> keys;
		std::vector<CacheRef> entries;
		for (auto e : res.items) {
			IndexCacheKey key(req.semantic_id, e->id);
			auto &node = nodes.at(key.get_node_id());
			keys.push_back(key);
			node_ids.insert(key.get_node_id());
			entries.push_back(CacheRef(node->host, node->port, key.get_entry_id(),e->bounds));
		}
		PuzzleRequest pr(
				req.type,
				req.semantic_id,
				res.covered,
				std::move(res.remainder), std::move(entries));

		// STATS ONLY
		if ( pr.has_remainders() && node_ids.size() == 1 ) {
			stats.multi_local_partials++;
		}
		else if ( pr.has_remainders() ) {
			stats.multi_remote_partials++;
		}
		if ( !pr.has_remainders() && node_ids.size() == 1 ) {
			stats.multi_local_hits++;
		}
		else if ( !pr.has_remainders() ) {
			stats.multi_remote_hits++;
		}
		// END STATS ONLY

		return make_unique<PuzzleJob>(std::move(pr), std::move(keys));
	}
	// Full miss
	else {
		stats.misses++;
		Log::debug("Full MISS.");
		return make_unique<CreateJob>(BaseRequest(req), *this);
	}
}

std::unique_ptr<PendingQuery> DefaultQueryManager::recreate_job(const RunningQuery& query) {
	auto &req = query.get_request();
	auto &cache = caches.get_cache(req.type);
	auto res = cache.query(req.semantic_id, req.query);
	auto job = create_job( req, res );
	job->add_clients( query.get_clients() );
	return job;
}

//
// Jobs
//

CreateJob::CreateJob( BaseRequest&& request, const DefaultQueryManager &mgr ) :
	PendingQuery(), request(request),
	orig_query(this->request.query),
	max_volume( QueryCube(this->request.query).volume() * 4.04 ),
	mgr(mgr) {
}

bool CreateJob::extend(const BaseRequest& req) {
	if ( req.type == request.type &&
		 req.semantic_id == request.semantic_id &&
		 orig_query.restype == req.query.restype) {

		QueryCube current(request.query);
		QueryCube requested(req.query);
		auto combined = current.combine(requested);

		if ( combined.volume() <= max_volume && (current.volume() + requested.volume()) * 1.01 >= combined.volume() ) {
			SpatialReference sref(orig_query.epsg, combined.get_dimension(0).a,
												   combined.get_dimension(1).a,
												   combined.get_dimension(0).b,
												   combined.get_dimension(1).b);

			TemporalReference tref(orig_query.timetype, combined.get_dimension(2).a, combined.get_dimension(2).b);

			if ( orig_query.restype == QueryResolution::Type::NONE ) {
				request.query = QueryRectangle(sref,tref,QueryResolution::none());
				return true;
			}
			else if ( orig_query.TemporalReference::contains(tref) ) {
				// Check resolution
				double my_xres = (orig_query.x2 - orig_query.x1) / orig_query.xres;
				double my_yres = (orig_query.y2 - orig_query.y1) / orig_query.yres;

				double q_xres = (req.query.x2 - req.query.x1) / req.query.xres;
				double q_yres = (req.query.y2 - req.query.y1) / req.query.yres;

				if (std::abs(1.0 - my_xres / q_xres) < 0.01 && std::abs(1.0 - my_yres / q_yres) < 0.01) {

					uint32_t nxres = std::ceil(
						orig_query.xres * ((sref.x2 - sref.x1) / (orig_query.x2 - orig_query.x1)));
					uint32_t nyres = std::ceil(
						orig_query.yres * ((sref.y2 - sref.y1) / (orig_query.y2 - orig_query.y1)));

					request.query = QueryRectangle(sref, tref, QueryResolution::pixels(nxres, nyres));
					return true;
				}
			}
		}
	}
	return false;
}


//bool CreateJob::extend(const BaseRequest& req) {
//	if ( req.type == request.type &&
//		 req.semantic_id == request.semantic_id &&
//		 orig_query.TemporalReference::contains(req.query) &&
//		 orig_query.restype == req.query.restype) {
//
//		double nx1, nx2, ny1, ny2, narea;
//
//		nx1 = std::min(request.query.x1, req.query.x1);
//		ny1 = std::min(request.query.y1, req.query.y1);
//		nx2 = std::max(request.query.x2, req.query.x2);
//		ny2 = std::max(request.query.y2, req.query.y2);
//
//		narea = (nx2 - nx1) * (ny2 - ny1);
//
//
//		if (orig_query.restype == QueryResolution::Type::NONE && narea / orig_area <= 4.01) {
//			SpatialReference sref(orig_query.epsg, nx1, ny1, nx2, ny2);
//			request.query = QueryRectangle(sref, orig_query, orig_query);
//			return true;
//		}
//		else if (orig_query.restype == QueryResolution::Type::PIXELS && narea / orig_area <= 4.01) {
//			// Check resolution
//			double my_xres = (orig_query.x2 - orig_query.x1) / orig_query.xres;
//			double my_yres = (orig_query.y2 - orig_query.y1) / orig_query.yres;
//
//			double q_xres = (req.query.x2 - req.query.x1) / req.query.xres;
//			double q_yres = (req.query.y2 - req.query.y1) / req.query.yres;
//
//			if (std::abs(1.0 - my_xres / q_xres) < 0.01 && std::abs(1.0 - my_yres / q_yres) < 0.01) {
//
//				uint32_t nxres = std::ceil(
//					orig_query.xres * ((nx2 - nx1) / (orig_query.x2 - orig_query.x1)));
//				uint32_t nyres = std::ceil(
//					orig_query.yres * ((ny2 - ny1) / (orig_query.y2 - orig_query.y1)));
//
//				SpatialReference sref(orig_query.epsg, nx1, ny1, nx2, ny2);
//				request.query = QueryRectangle(sref, orig_query, QueryResolution::pixels(nxres, nyres));
//				return true;
//			}
//		}
//	}
//	return false;
//}

bool CreateJob::is_affected_by_node(uint32_t node_id) {
	(void) node_id;
	return false;
}

uint64_t CreateJob::submit(const std::map<uint32_t, std::shared_ptr<Node> >& nmap) {
	uint32_t node_id = mgr.caches.find_node_for_job(request,mgr.nodes);
	uint64_t worker = nmap.at(node_id)->schedule_request(WorkerConnection::CMD_CREATE,request);
	if ( worker == 0 ) {
		for ( auto i = nmap.begin(); i != nmap.end() && worker == 0; i++ ) {
			worker = i->second->schedule_request(WorkerConnection::CMD_CREATE,request);
		}
	}
	return worker;
}

const BaseRequest& CreateJob::get_request() const {
	return request;
}

//
// DELIVCER JOB
//

DeliverJob::DeliverJob(DeliveryRequest&& request, const IndexCacheKey &key) :
	PendingQuery(),
	request(request),
	node(key.get_node_id()) {
}

bool DeliverJob::is_affected_by_node(uint32_t node_id) {
	return node_id == node;
}

bool DeliverJob::extend(const BaseRequest&) {
	return false;
}

uint64_t DeliverJob::submit(const std::map<uint32_t, std::shared_ptr<Node> >& nmap) {
	return nmap.at(node)->schedule_request(WorkerConnection::CMD_DELIVER,request);
}

const BaseRequest& DeliverJob::get_request() const {
	return request;
}

//
// PUZZLE JOB
//

PuzzleJob::PuzzleJob(PuzzleRequest&& request, const std::vector<IndexCacheKey> &keys) :
	PendingQuery(),
	request(std::move(request)), keys(keys) {
	for ( auto &k : keys ) {
		if ( nodes.insert(k.get_node_id()).second )
			nodes_priorized.push_back(k.get_node_id() );
	}
}

bool PuzzleJob::is_affected_by_node(uint32_t node_id) {
	return nodes.find(node_id) != nodes.end();
}


bool PuzzleJob::extend(const BaseRequest&) {
	return false;
}

uint64_t PuzzleJob::submit(
		const std::map<uint32_t, std::shared_ptr<Node> >& nmap) {
	uint64_t worker = 0;
	for ( auto &nid : nodes_priorized ) {
		worker = nmap.at(nid)->schedule_request(WorkerConnection::CMD_PUZZLE,request);
		if ( worker > 0 )
			break;
	}
	return worker;

}

const BaseRequest& PuzzleJob::get_request() const {
	return request;
}
