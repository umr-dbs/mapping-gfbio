/*
 * node.cpp
 *
 *  Created on: 11.03.2016
 *      Author: mika
 */

#include "cache/index/node.h"
#include "cache/common.h"
#include "util/concat.h"
#include "util/exceptions.h"

////////////////////////////////////////////////////////////
//
// NODE
//
////////////////////////////////////////////////////////////

Node::Node(uint32_t id, const std::string &host, const NodeHandshake &hs) :
	id(id), host(host), port(hs.port), last_stat_update( CacheCommon::time_millis() ), control_connection(0) {
	for ( auto &cu : hs.get_data() ) {
		usage.emplace(cu.type,CacheUsage(cu));
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



std::string Node::to_string() const {
	std::ostringstream ss;
	ss << "Node " << id << "[" << std::endl;
	ss << "  " << query_stats.to_string() << std::endl;
	ss << "]";
	return ss.str();
}

