/*
 * util.cpp
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#include "cache/node/node_manager.h"
#include "cache/node/manager/local_manager.h"
#include "cache/node/manager/remote_manager.h"
#include "cache/node/manager/hybrid_manager.h"
#include "cache/node/puzzle_util.h"
#include "cache/priv/connection.h"

#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

////////////////////////////////////////////////////////////
//
// ActiveQueryStats
//
////////////////////////////////////////////////////////////

void ActiveQueryStats::add_single_local_hit() {
	std::lock_guard<std::mutex> g(mtx);
	single_local_hits++;
}

void ActiveQueryStats::add_multi_local_hit() {
	std::lock_guard<std::mutex> g(mtx);
	multi_local_hits++;
}

void ActiveQueryStats::add_multi_local_partial() {
	std::lock_guard<std::mutex> g(mtx);
	multi_local_partials++;
}

void ActiveQueryStats::add_single_remote_hit() {
	std::lock_guard<std::mutex> g(mtx);
	single_remote_hits++;
}

void ActiveQueryStats::add_multi_remote_hit() {
	std::lock_guard<std::mutex> g(mtx);
	multi_remote_hits++;
}

void ActiveQueryStats::add_multi_remote_partial() {
	std::lock_guard<std::mutex> g(mtx);
	multi_remote_partials++;
}

void ActiveQueryStats::add_miss() {
	std::lock_guard<std::mutex> g(mtx);
	misses++;
}

QueryStats ActiveQueryStats::get() const {
	std::lock_guard<std::mutex> g(mtx);
	return QueryStats(*this);
}

void ActiveQueryStats::add_query(double ratio) {
	std::lock_guard<std::mutex> g(mtx);
	QueryStats::add_query(ratio);
}

QueryStats ActiveQueryStats::get_and_reset() {
	std::lock_guard<std::mutex> g(mtx);
	auto res = QueryStats(*this);
	reset();
	return res;
}


////////////////////////////////////////////////////////////
//
// WorkerContext
//
////////////////////////////////////////////////////////////

WorkerContext::WorkerContext() : index_connection(nullptr) {
}

BlockingConnection& WorkerContext::get_index_connection() const {
	if ( index_connection == nullptr )
		throw IllegalStateException("No index-connection configured for this thread");
	return *index_connection;
}

void WorkerContext::set_index_connection(BlockingConnection *con) {
	index_connection = con;
}

////////////////////////////////////////////////////////////
//
// NodeCacheWrapper
//
////////////////////////////////////////////////////////////


template<typename T>
NodeCacheWrapper<T>::NodeCacheWrapper( NodeCacheManager &mgr, size_t size, CacheType type ) :
	mgr(mgr), cache(type,size) {
}


template<typename T>
std::shared_ptr<const NodeCacheEntry<T>> NodeCacheWrapper<T>::get(const NodeCacheKey &key) const {
	Log::debug("Getting item from local cache. Key: %s", key.to_string().c_str());
	return cache.get(key);
}

template<typename T>
QueryStats NodeCacheWrapper<T>::get_and_reset_query_stats() {
	return stats.get_and_reset();
}


template<typename T>
CacheType NodeCacheWrapper<T>::get_type() const {
	return cache.type;
}

////////////////////////////////////////////////////////////
//
// NodeCacheManager
//
////////////////////////////////////////////////////////////

thread_local WorkerContext NodeCacheManager::context;

NodeCacheManager::NodeCacheManager( const std::string &strategy,
	std::unique_ptr<NodeCacheWrapper<GenericRaster>> raster_wrapper,
	std::unique_ptr<NodeCacheWrapper<PointCollection>> point_wrapper,
	std::unique_ptr<NodeCacheWrapper<LineCollection>> line_wrapper,
	std::unique_ptr<NodeCacheWrapper<PolygonCollection>> polygon_wrapper,
	std::unique_ptr<NodeCacheWrapper<GenericPlot>> plot_wrapper ) :
	raster_wrapper( std::move(raster_wrapper) ),
	point_wrapper( std::move(point_wrapper) ),
	line_wrapper( std::move(line_wrapper) ),
	polygon_wrapper( std::move(polygon_wrapper) ),
	plot_wrapper( std::move(plot_wrapper) ),
	strategy( CachingStrategy::by_name(strategy)),
	my_port(0) {
}

NodeCacheWrapper<GenericRaster>& NodeCacheManager::get_raster_cache() {
	return *raster_wrapper;
}

NodeCacheWrapper<PointCollection>& NodeCacheManager::get_point_cache() {
	return *point_wrapper;
}

NodeCacheWrapper<LineCollection>& NodeCacheManager::get_line_cache() {
	return *line_wrapper;
}

NodeCacheWrapper<PolygonCollection>& NodeCacheManager::get_polygon_cache() {
	return *polygon_wrapper;
}

NodeCacheWrapper<GenericPlot>& NodeCacheManager::get_plot_cache() {
	return *plot_wrapper;
}



std::unique_ptr<NodeCacheManager> NodeCacheManager::by_name(
		const std::string& name, size_t raster_size, size_t point_size, size_t line_size,
		size_t polygon_size, size_t plot_size, const std::string &strategy, const std::string& local_replacement) {
	std::string mgrlc;
	mgrlc.resize(name.size());
	std::transform(name.cbegin(),name.cend(),mgrlc.begin(),::tolower);


	if ( mgrlc == "remote" )
		return make_unique<RemoteCacheManager>(strategy, raster_size, point_size, line_size, polygon_size, plot_size);
	else if ( mgrlc == "local" )
		return make_unique<LocalCacheManager>(strategy, local_replacement, raster_size, point_size, line_size, polygon_size, plot_size);
	else if ( mgrlc == "hybrid" )
		return make_unique<HybridCacheManager>(strategy, raster_size, point_size, line_size, polygon_size, plot_size);
	else
		throw ArgumentException(concat("Unknown manager impl: ", name));
}


void NodeCacheManager::set_self_port(uint32_t port) {
	my_port = port;
}

void NodeCacheManager::set_self_host(const std::string& host) {
	my_host = host;
}

NodeHandshake NodeCacheManager::create_handshake() const {
	std::vector<CacheHandshake> hs {
		raster_wrapper->cache.get_all(),
		point_wrapper->cache.get_all(),
		line_wrapper->cache.get_all(),
		polygon_wrapper->cache.get_all(),
		plot_wrapper->cache.get_all()
	};
	return NodeHandshake(my_port, std::move(hs) );
}

NodeStats NodeCacheManager::get_stats_delta() const {
	QueryStats qs;
	qs += raster_wrapper->get_and_reset_query_stats();
	qs += point_wrapper->get_and_reset_query_stats();
	qs += line_wrapper->get_and_reset_query_stats();
	qs += polygon_wrapper->get_and_reset_query_stats();
	qs += plot_wrapper->get_and_reset_query_stats();

	cumulated_stats += qs;
	std::vector<CacheStats> stats {
		raster_wrapper->cache.get_stats(),
		point_wrapper->cache.get_stats(),
		line_wrapper->cache.get_stats(),
		polygon_wrapper->cache.get_stats(),
		plot_wrapper->cache.get_stats()
	};
	return NodeStats( qs, std::move(stats) );
}

QueryStats NodeCacheManager::get_cumulated_query_stats() const {
	return cumulated_stats;
}

WorkerContext& NodeCacheManager::get_worker_context() {
	return NodeCacheManager::context;
}

const CachingStrategy& NodeCacheManager::get_strategy() const {
	return *strategy;
}

template class NodeCacheWrapper<GenericRaster>;
template class NodeCacheWrapper<PointCollection>;
template class NodeCacheWrapper<LineCollection>;
template class NodeCacheWrapper<PolygonCollection>;
template class NodeCacheWrapper<GenericPlot> ;

