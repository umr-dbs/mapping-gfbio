/*
 * bema_query_manager.cpp
 *
 *  Created on: 24.05.2016
 *      Author: koerberm
 */

#include "dema_query_manager.h"

#include "util/exceptions.h"

DemaQueryManager::DemaQueryManager(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) : QueryManager(nodes), alpha(0.3) {
}

void DemaQueryManager::add_request(uint64_t client_id, const BaseRequest& req) {
	auto j = create_job(req);
	j->add_client(client_id);
	pending_jobs.push_back( std::move(j) );
}

void DemaQueryManager::process_worker_query(WorkerConnection& con) {
	(void) con;
	throw MustNotHappenException("No worker-queries allowed in BEMA-scheduling! Check your node-configuration!");
}

std::unique_ptr<PendingQuery> DemaQueryManager::recreate_job(const RunningQuery& query) {
	auto res = create_job(query.get_request());
	res->add_clients(query.get_clients());
	return res;
}

std::unique_ptr<PendingQuery> DemaQueryManager::create_job(
		const BaseRequest& req) {

	auto &q = req.query;
	double px = q.x1 + (q.x2-q.x1)/2;
	double py = q.y1 + (q.y2-q.y1)/2;

	Point2 qc( px, py );

	double min_dist = std::numeric_limits<double>::max();
	uint32_t node_id = 0;

	for ( auto &p : nodes ) {
		auto sit = infos.find(p.first);
		if ( sit == infos.end() ) {
			infos.emplace(p.first, ServerInfo(qc) );
			return make_unique<DemaJob>(req,p.first);
		}
		else {
			auto &si = sit->second;
			double cdist = qc.distance_to(si.p);
			if ( cdist < min_dist ) {
				min_dist = cdist;
				node_id = sit->first;
			}
		}
	}
	auto &si = infos.at(node_id);
	si.p = (qc*alpha) + (si.p * (1-alpha));
	return make_unique<DemaJob>(req,node_id);
}

//
//
//

DemaJob::DemaJob(const BaseRequest &request, uint32_t node_id) : PendingQuery(), request(request), node_id(node_id) {
}

uint64_t DemaJob::schedule(
		const std::map<uint64_t, std::unique_ptr<WorkerConnection> >& connections) {
	for (auto &e : connections) {
		auto &con = *e.second;
		if (!con.is_faulty() && con.node_id == node_id && con.get_state() == WorkerState::IDLE) {
			con.process_request(WorkerConnection::CMD_CREATE, request);
			return con.id;
		}
	}
	return 0;
}

bool DemaJob::is_affected_by_node(uint32_t node_id) {
	return node_id == this->node_id;
}

bool DemaJob::extend(const BaseRequest& req) {
	(void) req;
	return false;
}

const BaseRequest& DemaJob::get_request() const {
	return request;
}
