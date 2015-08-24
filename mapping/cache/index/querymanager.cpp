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


//
//
//

QueryManager::~QueryManager() {
}

QueryManager::QueryManager(const IndexCache& raster_cache, const std::map<uint32_t,std::shared_ptr<Node>> &nodes) :
	raster_cache(raster_cache), nodes(nodes) {
}

void QueryManager::add_raster_request(uint64_t client_id, const BaseRequest& req) {
	// Check if running jobs satisfy the given query
	for ( auto &qi : queries ) {
		if ( qi.second.satisfies(req) ) {
			qi.second.add_client(client_id);
			return;
		}
	}

	// Check if pending jobs satisfy the given query
	for ( auto &j : pending_jobs ) {
		if ( j->satisfies(req) ) {
			j->add_client(client_id);
			return;
		}
	}

	// Check if a pending query may be extended by the given query
	for ( auto &j : pending_jobs ) {
		if ( j->extend(req) ) {
			j->add_client(client_id);
			return;
		}
	}



	auto res = raster_cache.query(req.semantic_id, req.query);
	Log::debug("QueryResult: %s", res.to_string().c_str());

	// Full single hit
	if (res.keys.size() == 1 && !res.has_remainder()) {
		Log::debug("Full HIT. Sending reference.");

		IndexCacheKey key(req.semantic_id,res.keys.at(0));
		auto ref = raster_cache.get(key);
		std::unique_ptr<DeliveryRequest> jreq = make_unique<DeliveryRequest>(req.semantic_id,
			req.query, ref.entry_id);
		pending_jobs.push_back(make_unique<DeliverJob>(client_id, jreq, ref.node_id));
	}
	// Puzzle
	else if (res.has_hit() && res.coverage > 0.1) {
		Log::debug("Partial HIT. Sending puzzle-request, coverage: %f", res.coverage);
		std::vector<uint32_t> node_ids;
		std::vector<CacheRef> entries;
		for (auto id : res.keys) {
			IndexCacheKey key(req.semantic_id,id);
			auto ref = raster_cache.get(key);
			auto &node = nodes.at(ref.node_id);
			node_ids.push_back(ref.node_id);
			entries.push_back(CacheRef(node->host, node->port, ref.entry_id));
		}
		std::unique_ptr<PuzzleRequest> jreq = make_unique<PuzzleRequest>(req.semantic_id, req.query,
			res.covered, res.remainder, entries);
		pending_jobs.push_back(make_unique<PuzzleJob>(client_id, jreq, node_ids));
	}
	// Full miss
	else {
		Log::debug("Full MISS.");
		std::unique_ptr<BaseRequest> jreq = make_unique<BaseRequest>(req);
		pending_jobs.push_back(make_unique<CreateJob>(client_id, jreq));
	}
}

void QueryManager::schedule_pending_jobs(
	const std::map<uint64_t, std::unique_ptr<WorkerConnection> > &worker_connections) {

	auto it = pending_jobs.begin();
	while (it != pending_jobs.end()) {
		uint64_t con_id = (*it)->schedule(worker_connections);
		if ( con_id != 0 ) {
			queries.emplace(con_id,QueryInfo(**it));
			it = pending_jobs.erase(it);
		}
		else
			++it;
	}
}


size_t QueryManager::close_worker(uint64_t worker_id) {
	finished_queries.emplace(worker_id, queries.at(worker_id));
	queries.erase(worker_id);
	return finished_queries.at(worker_id).get_clients().size();
}



std::vector<uint64_t> QueryManager::release_worker(uint64_t worker_id) {
	std::vector<uint64_t> clients = finished_queries.at(worker_id).get_clients();
	finished_queries.erase(worker_id);
	return clients;
}

//
// Jobs
//


QueryInfo::QueryInfo(const BaseRequest& request, uint64_t client) :
	query(request.query), semantic_id(request.semantic_id) {
	clients.push_back(client);
}

QueryInfo::QueryInfo(const QueryRectangle& query, const std::string& semantic_id, uint64_t client) :
	query(query), semantic_id(semantic_id) {
	clients.push_back(client);
}

bool QueryInfo::satisfies(const BaseRequest& req) {
	if ( req.semantic_id == semantic_id  &&
		 query.SpatialReference::contains(req.query) &&
		 query.TemporalReference::contains(req.query) &&
		 query.restype == req.query.restype ) {

		if ( query.restype == QueryResolution::Type::NONE )
			return true;
		else if (query.restype == QueryResolution::Type::PIXELS) {
			// Check resolution
			double my_xres = (query.x2-query.x1) / query.xres;
			double my_yres = (query.y2-query.y1) / query.yres;

			double q_xres = (req.query.x2-req.query.x1) / req.query.xres;
			double q_yres = (req.query.y2-req.query.y1) / req.query.yres;

			return std::abs(1.0 - my_xres/q_xres) < 0.01 &&
				   std::abs(1.0 - my_yres/q_yres) < 0.01;
		}
		else
			throw ArgumentException("Unknown QueryResolution::Type in QueryRectangle");
	}
	return false;
}

void QueryInfo::add_client(uint64_t client) {
	clients.push_back(client);
}

const std::vector<uint64_t>& QueryInfo::get_clients() {
	return clients;
}

JobDescription::~JobDescription() {
}

bool JobDescription::extend(const BaseRequest& req) {
	(void) req;
	return false;
}

JobDescription::JobDescription( uint64_t client_id, std::unique_ptr<BaseRequest> request) :
	QueryInfo(*request, client_id), request(std::move(request)) {
}

CreateJob::CreateJob(uint64_t client_id, std::unique_ptr<BaseRequest>& request) :
	JobDescription(client_id, std::unique_ptr<BaseRequest>(request.release())), orig_query(query),
	orig_area((query.x2-query.x1)*(query.y2-query.y1)) {
}

CreateJob::~CreateJob() {
}

bool CreateJob::extend(const BaseRequest& req) {
	if ( req.semantic_id == semantic_id  &&
		 orig_query.TemporalReference::contains(req.query) &&
		 orig_query.restype == req.query.restype ) {

		double nx1,nx2,ny1,ny2, narea;

		nx1 = std::min(query.x1,req.query.x1);
		ny1 = std::min(query.y1,req.query.y1);
		nx2 = std::max(query.x2,req.query.x2);
		ny2 = std::max(query.y2,req.query.y2);

		narea = (nx2-nx1) * (ny2-ny1);

		if ( orig_query.restype == QueryResolution::Type::NONE && narea / orig_area <= 4 ) {
			SpatialReference sref( orig_query.epsg,nx1,ny1,nx2,ny2 );
			query = QueryRectangle( sref, orig_query, orig_query );
			return true;
		}
		else if ( orig_query.restype == QueryResolution::Type::PIXELS && narea / orig_area <= 4 ) {
			// Check resolution
			double my_xres = (orig_query.x2-orig_query.x1) / orig_query.xres;
			double my_yres = (orig_query.y2-orig_query.y1) / orig_query.yres;

			double q_xres = (req.query.x2-req.query.x1) / req.query.xres;
			double q_yres = (req.query.y2-req.query.y1) / req.query.yres;

			if ( std::abs(1.0 - my_xres/q_xres) < 0.01 &&
				 std::abs(1.0 - my_yres/q_yres) < 0.01 ) {

				uint32_t nxres = std::round(orig_query.xres * ((nx2-nx1) / (orig_query.x2-orig_query.x1)));
				uint32_t nyres = std::round(orig_query.yres * ((ny2-ny1) / (orig_query.y2-orig_query.y1)));

				SpatialReference sref( orig_query.epsg,nx1,ny1,nx2,ny2 );
				query = QueryRectangle( sref, orig_query, QueryResolution::pixels(nxres,nyres) );
				request.reset( new BaseRequest(semantic_id,query) );
				return true;
			}
		}
	}
	return false;
}

uint64_t  CreateJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.get_state() == WorkerConnection::State::IDLE) {
			con.process_request(WorkerConnection::CMD_CREATE_RASTER, *request);
			return con.id;
		}
	}
	return 0;
}

DeliverJob::DeliverJob(uint64_t client_id, std::unique_ptr<DeliveryRequest>& request,
	uint32_t node) :
	JobDescription(client_id, std::unique_ptr<BaseRequest>(request.release())), node(node) {
}

DeliverJob::~DeliverJob() {
}

uint64_t  DeliverJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.node->id == node && con.get_state() == WorkerConnection::State::IDLE) {
			con.process_request(WorkerConnection::CMD_DELIVER_RASTER, *request);
			return con.id;
		}
	}
	return 0;
}

PuzzleJob::PuzzleJob(uint64_t client_id, std::unique_ptr<PuzzleRequest>& request, std::vector<uint32_t>& nodes) :
	JobDescription(client_id, std::unique_ptr<BaseRequest>(request.release())), nodes(std::move(nodes)) {
}

PuzzleJob::~PuzzleJob() {
}

uint64_t  PuzzleJob::schedule(const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &node : nodes) {
		for (auto &e : connections) {
			auto &con = *e.second;
			if (!con.is_faulty() && con.node->id == node
				&& con.get_state() == WorkerConnection::State::IDLE) {
				con.process_request(WorkerConnection::CMD_PUZZLE_RASTER, *request);
				return con.id;
			}
		}
	}
	return 0;
}





