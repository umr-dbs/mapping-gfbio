/*
 * util.cpp
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#include "cache/node/util.h"
#include "cache/manager.h"

std::unique_ptr<NodeUtil> NodeUtil::instance( new NodeUtil() );

NodeUtil& NodeUtil::get_instance() {
	return *instance;
}

void NodeUtil::set_instance(std::unique_ptr<NodeUtil> inst) {
	instance = std::move(inst);
}

//
// INSTANCE
//

thread_local BinaryStream *NodeUtil::index_connection = nullptr;

NodeUtil::NodeUtil() : my_port(0) {
}

void NodeUtil::set_self_port(uint32_t port) {
	my_port = port;
}

void NodeUtil::set_self_host(const std::string& host) {
	my_host = host;
}

CacheRef NodeUtil::create_self_ref(uint64_t id) const {
	return CacheRef( my_host, my_port, id );
}

bool NodeUtil::is_self_ref(const CacheRef& ref) const {
	return ref.host == my_host && ref.port == my_port;
}

NodeHandshake NodeUtil::create_handshake() const {
	auto &cm = CacheManager::get_instance();
	Capacity cap(
		cm.get_raster_cache().get_max_size(), cm.get_raster_cache().get_current_size(),
		cm.get_point_cache().get_max_size(), cm.get_point_cache().get_current_size(),
		cm.get_line_cache().get_max_size(), cm.get_line_cache().get_current_size(),
		cm.get_polygon_cache().get_max_size(), cm.get_polygon_cache().get_current_size(),
		cm.get_plot_cache().get_max_size(), cm.get_plot_cache().get_current_size()
	);

	std::vector<NodeCacheRef> entries = cm.get_raster_cache().get_all();
	std::vector<NodeCacheRef> tmp = cm.get_point_cache().get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = cm.get_line_cache().get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = cm.get_polygon_cache().get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	tmp = cm.get_plot_cache().get_all();
	entries.insert(entries.end(), tmp.begin(), tmp.end() );

	return NodeHandshake(my_port, cap, entries );
}

NodeStats NodeUtil::get_stats() const {
	auto &cm = CacheManager::get_instance();
	Capacity cap(
		cm.get_raster_cache().get_max_size(), cm.get_raster_cache().get_current_size(),
		cm.get_point_cache().get_max_size(), cm.get_point_cache().get_current_size(),
		cm.get_line_cache().get_max_size(), cm.get_line_cache().get_current_size(),
		cm.get_polygon_cache().get_max_size(), cm.get_polygon_cache().get_current_size(),
		cm.get_plot_cache().get_max_size(), cm.get_plot_cache().get_current_size()
	);


	std::vector<CacheStats> stats {
		cm.get_raster_cache().get_stats(),
		cm.get_point_cache().get_stats(),
		cm.get_line_cache().get_stats(),
		cm.get_polygon_cache().get_stats(),
		cm.get_plot_cache().get_stats()
	};

	return NodeStats( cap, stats );
}

void NodeUtil::set_index_connection(BinaryStream* con) {
	NodeUtil::index_connection = con;
}

BinaryStream& NodeUtil::get_index_connection() {
	if ( NodeUtil::index_connection == nullptr )
		throw IllegalStateException("No index-connection configured for this thread");
	return *NodeUtil::index_connection;
}

