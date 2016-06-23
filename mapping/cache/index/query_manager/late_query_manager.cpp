/*
 * late_query_manager.cpp
 *
 *  Created on: 17.06.2016
 *      Author: koerberm
 */

#include "cache/index/query_manager/late_query_manager.h"

LateJob::LateJob(const BaseRequest& request, IndexCacheManager &caches, SystemStats &stats) : caches(caches), request(request),
	orig_query(this->request.query),
	max_volume( QueryCube(this->request.query).volume() * 4.04 ),
	stats(stats) {
}

bool LateJob::extend(const BaseRequest& req) {
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

bool LateJob::is_affected_by_node(uint32_t node_id) {
	(void) node_id;
	return false;
}

const BaseRequest& LateJob::get_request() const {
	return request;
}

uint64_t LateJob::submit(const std::map<uint32_t, std::shared_ptr<Node> >& nmap) {

	QueryStats tmp;

	IndexCache &cache = caches.get_cache(request.type);
	auto res = cache.query( request.semantic_id, request.query );

	uint64_t worker = 0;

	// Full single hit
	if (res.items.size() == 1 && !res.has_remainder()) {
		tmp.single_local_hits++;
		Log::debug("Full HIT. Sending reference.");
		IndexCacheKey key(request.semantic_id, res.items.front()->id);
		DeliveryRequest dr(
				request.type,
				request.semantic_id,
				res.covered,
				key.get_entry_id());
		worker = nmap.at(key.id.first)->schedule_request(WorkerConnection::CMD_DELIVER,dr);
	}
	// Puzzle
	else if (res.has_hit()) {
		Log::debug("Partial HIT. Sending puzzle-request.");
		std::set<uint32_t> node_ids;
		std::vector<uint32_t> prio_nodes;
		std::vector<IndexCacheKey> keys;
		std::vector<CacheRef> entries;
		for (auto e : res.items) {
			IndexCacheKey key(request.semantic_id, e->id);
			auto &node = nmap.at(key.get_node_id());
			keys.push_back(key);
			if ( node_ids.emplace(key.get_node_id()).second )
				prio_nodes.push_back(key.get_node_id());
			entries.push_back(CacheRef(node->host, node->port, key.get_entry_id(),e->bounds));
		}
		PuzzleRequest pr(
				request.type,
				request.semantic_id,
				res.covered,
				std::move(res.remainder), std::move(entries));

		// STATS ONLY
		if ( pr.has_remainders() && node_ids.size() == 1 ) {
			tmp.multi_local_partials++;
		}
		else if ( pr.has_remainders() ) {
			tmp.multi_remote_partials++;
		}
		if ( !pr.has_remainders() && node_ids.size() == 1 ) {
			tmp.multi_local_hits++;
		}
		else if ( !pr.has_remainders() ) {
			tmp.multi_remote_hits++;
		}
		// END STATS ONLY

		for ( auto i = prio_nodes.begin(); worker == 0 && i != prio_nodes.end(); i++ ) {
			worker = nmap.at(*i)->schedule_request(WorkerConnection::CMD_PUZZLE,pr);
		}
	}
	// Full miss
	else {
		tmp.misses++;
		Log::debug("Full MISS.");
		uint32_t node_id = caches.find_node_for_job(request,nmap);
		worker = nmap.at(node_id)->schedule_request(WorkerConnection::CMD_CREATE,request);
		if ( worker == 0 ) {
			for ( auto i = nmap.begin(); worker == 0 && i != nmap.end(); i++ ) {
				worker = i->second->schedule_request(WorkerConnection::CMD_CREATE,request);
			}
		}
	}

	if ( worker > 0 ) {
		stats += tmp;
	}
	return worker;
}

/*
 * MANAGER
 */

LateQueryManager::LateQueryManager(
		const std::map<uint32_t, std::shared_ptr<Node> >& nodes,
		IndexCacheManager& caches, bool enable_batching) : QueryManager(nodes), caches(caches), enable_batching(enable_batching) {
}

void LateQueryManager::add_request(uint64_t client_id, const BaseRequest& req) {
	stats.issued();
	if ( enable_batching ) {
		TIME_EXEC("QueryManager.add_request");
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
	}
	// Create a new job
	auto job = make_unique<LateJob>(req,caches,stats);
	job->add_client(client_id);
	add_query(std::move(job));
}

void LateQueryManager::process_worker_query(WorkerConnection& con) {
	auto &req = con.get_query();
	try {
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
				entries.push_back(CacheRef(node->host, node->port, e->id.second,e->bounds));
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

bool LateQueryManager::use_reorg() const {
	return true;
}

std::unique_ptr<PendingQuery> LateQueryManager::recreate_job(
		const RunningQuery& query) {
	std::unique_ptr<PendingQuery> res = make_unique<LateJob>(query.get_request(),caches,stats);
	res->add_clients(query.get_clients());
	return res;
}
