/*
 * node.cpp
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#include "cache/index/node.h"
#include "cache/index/querymanager.h"
#include "cache/common.h"
#include "util/concat.h"
#include "util/exceptions.h"


////////////////////////////////////////////////////////////
//
// NODE
//
////////////////////////////////////////////////////////////

Node::Node(uint32_t id, const std::string &host, const NodeHandshake &hs, std::unique_ptr<ControlConnection> cc ) :
	id(id), host(host), port(hs.port),
	control_connection(std::move(cc)),
	_last_stats_request( CacheCommon::time_millis() ) {
	for ( auto &cu : hs.get_data() ) {
		usage.emplace(cu.type,CacheUsage(cu));
	}
}

void Node::setup_connections(struct pollfd* fds, size_t& pos, QueryManager &query_manager) {
	ControlConnection &cc = *control_connection;
	if (cc.is_faulty()) {
		throw NodeFailedException("ControlConnection is faulty!");
	}
	else {
		cc.prepare(&fds[pos++]);
	}


	auto wit = busy_workers.begin();
	while (wit != busy_workers.end()) {
		WorkerConnection &wc = *wit->second;
		if (wc.is_faulty()) {
			query_manager.worker_failed(wc.id);
			wit = busy_workers.erase(wit);
		}
		else {
			wc.prepare(&fds[pos++]);
			wit++;
		}
	}
}


void Node::update_stats(const NodeStats& stats) {
	for ( auto &cu : stats.stats ) {
		usage.at(cu.type) = CacheUsage(cu);
	}
	query_stats += stats.query_stats;
}


const CacheUsage& Node::get_usage(CacheType type) const {
	return usage.at(type);
}

const QueryStats& Node::get_query_stats() const {
	return query_stats;
}

void Node::reset_query_stats() {
	query_stats.reset();
}


void Node::send_stats_request() {
	if ( is_control_connection_idle() ) {
		_last_stats_request = CacheCommon::time_millis();
		control_connection->send_get_stats();
	}
}

void Node::send_reorg(const ReorgDescription& desc) {
	control_connection->send_reorg(desc);
}

time_t Node::last_stats_request() const {
	return _last_stats_request;
}

bool Node::is_control_connection_idle() const {
	return control_connection->get_state() == ControlState::IDLE;
}

void Node::add_worker(std::unique_ptr<WorkerConnection> worker) {
	idle_workers.push_back( std::move(worker) );
}

ControlConnection& Node::get_control_connection() {
	return *control_connection;
}

uint32_t Node::num_idle_workers() const {
	return idle_workers.size();
}

std::map<uint64_t, std::unique_ptr<WorkerConnection> >& Node::get_busy_workers() {
	return busy_workers;
}

std::string Node::to_string() const {
	std::ostringstream ss;
	ss << "Node " << id << "[" << std::endl;
	ss << "  " << query_stats.to_string() << std::endl;
	ss << "  busy workers: ";
	for ( auto &p : busy_workers ) {
		ss << p.first << ",";
	}
	ss << std::endl;
	ss << "  idle workers: ";
	for ( auto &w : idle_workers ) {
		ss << w->id << ",";
	}
	ss << std::endl;
	ss << "]";
	return ss.str();
}

bool Node::has_idle_worker() const {
	return !idle_workers.empty();
}

uint64_t Node::schedule_request( uint8_t cmd, const BaseRequest& req) {
	if ( !idle_workers.empty() ) {
		auto &wc = idle_workers.back();
		auto id = wc->id;
		wc->process_request(cmd,req);
		busy_workers.emplace(id,std::move(wc));
		idle_workers.pop_back();
		return id;
	}
	else
		return 0;
}

void Node::release_worker(uint64_t id) {
	auto it = busy_workers.find(id);
	if ( it != busy_workers.end() ) {
		it->second->release();
		idle_workers.push_back( std::move(it->second) );
		busy_workers.erase(it);
	}
	else
		Log::warn("No worker with id: %ul on node: %ud", id, this->id);
}

