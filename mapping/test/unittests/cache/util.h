/*
 * util.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef UNITTESTS_CACHE_UTIL_H_
#define UNITTESTS_CACHE_UTIL_H_

#include "cache/index/indexserver.h"
#include "cache/node/node_manager.h"
#include "cache/node/nodeserver.h"
#include "cache/manager.h"
#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"

#include <iostream>
#include <vector>
#include <string>
#include <cmath>
#include <thread>
#include <memory>

/**
 * This function converts a "datetime"-string in ISO8601 format into a time_t using UTC
 * @param dateTimeString a string with ISO8601 "datetime"
 * @returns The time_t representing the "datetime"
 */
time_t parseIso8601DateTime(std::string dateTimeString);

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg = EPSG_WEBMERCATOR, bool allow_infinite = false);
typedef std::unique_ptr<std::thread> TP;


QueryRectangle random_rect( epsg_t epsg, double extend, double time, uint32_t res );

class QTriple {
public:
	QTriple( CacheType type, const QueryRectangle &query, const std::string semantic_id ) :
		type(type), query(query), semantic_id(semantic_id) {};
	const CacheType type;
	const QueryRectangle query;
	const std::string semantic_id;
};

/**
 * Tracing implementation of cache manager
 */
template<class T, CacheType TYPE>
class TracingCacheWrapper : public CacheWrapper<T> {
public:
	TracingCacheWrapper( std::vector<QTriple> &query_log );
	void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);

	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
private:
	std::vector<QTriple> &query_log;
};

template class TracingCacheWrapper<GenericRaster,CacheType::RASTER>;

class TracingCacheManager: public CacheManager {
public:
	TracingCacheManager();
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
	std::vector<QTriple> query_log;
private:
	TracingCacheWrapper<GenericRaster,CacheType::RASTER> rw;
	TracingCacheWrapper<PointCollection,CacheType::POINT> pw;
	TracingCacheWrapper<LineCollection,CacheType::LINE> lw;
	TracingCacheWrapper<PolygonCollection,CacheType::POLYGON> pow;
	TracingCacheWrapper<GenericPlot,CacheType::PLOT> plw;
};


void execute_operator( GenericOperator &op, const QueryRectangle &query, CacheType type );

void execute( const QTriple &t );

std::vector<QTriple> get_query_steps( const std::string &semantic_id, const QueryRectangle &query, CacheType type );

void execute_query_steps( const std::vector<QTriple> &queries );

/**
 * Local implementation of cache manager
 */
template<class T>
class LocalCacheWrapper : public CacheWrapper<T> {
	friend class LocalCacheManager;
public:
	LocalCacheWrapper( NodeCache<T> &cache, std::unique_ptr<Puzzler<T>> puzzler, CachingStrategy *strategy );
	void put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(const GenericOperator &op, const QueryRectangle &rect);
private:
	std::unique_ptr<T> process_puzzle(const PuzzleRequest& request);
	NodeCache<T> &cache;
	LocalRetriever<T> retriever;
	PuzzleUtil<T> puzzle_util;
	CachingStrategy *strategy;
};


class LocalCacheManager : public CacheManager {
public:
	LocalCacheManager( std::unique_ptr<CachingStrategy> strategy,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();

	void set_strategy(std::unique_ptr<CachingStrategy> strategy);
private:
	NodeCache<GenericRaster> rc;
	NodeCache<PointCollection> pc;
	NodeCache<LineCollection> lc;
	NodeCache<PolygonCollection> poc;
	NodeCache<GenericPlot> plc;

	LocalCacheWrapper<GenericRaster> rw;
	LocalCacheWrapper<PointCollection> pw;
	LocalCacheWrapper<LineCollection> lw;
	LocalCacheWrapper<PolygonCollection> pow;
	LocalCacheWrapper<GenericPlot> plw;

	std::unique_ptr<CachingStrategy> strategy;
};

//
// Test classes
//

class TestIdxServer : public IndexServer {
public:
	void trigger_reorg( uint32_t node_id, const ReorgDescription &desc );

	void force_stat_update();

	void force_reorg();

	void reset_stats();

	TestIdxServer( uint32_t port, const std::string &reorg_strategy );
private:
	void wait_for_idle_control_connections();
};

class TestNodeServer : public NodeServer {
public:
	static void run_node_thread(TestNodeServer *ns);

	TestNodeServer(int num_threads, uint32_t my_port, const std::string &index_host, uint32_t index_port, const std::string &strategy, size_t capacity = 5 * 1024 * 1024 );
	NodeCacheManager &get_cache_manager();
	bool owns_current_thread();
	std::thread::id my_id;
};


class TestCacheMan : public CacheManager {
public:
	void add_instance( TestNodeServer *inst );
	NodeCacheManager& get_instance_mgr( int i );
	NodeCacheWrapper<GenericRaster>& get_raster_cache();
	NodeCacheWrapper<PointCollection>& get_point_cache();
	NodeCacheWrapper<LineCollection>& get_line_cache();
	NodeCacheWrapper<PolygonCollection>& get_polygon_cache();
	NodeCacheWrapper<GenericPlot>& get_plot_cache();
private:
	NodeCacheManager& get_current_instance() const;
	std::vector<TestNodeServer*> instances;
};



class LocalTestSetup {
public:
	LocalTestSetup(int num_nodes, int num_workers, size_t capacity, std::string reorg_strat, std::string c_strat );
	~LocalTestSetup();
	ClientCacheManager &get_client();
	TestIdxServer &get_index();
	std::vector<std::unique_ptr<TestNodeServer>>& get_nodes();
private:
	static const int INDEX_PORT = 12346;
	TestCacheMan mgr;
	ClientCacheManager ccm;
	std::unique_ptr<TestIdxServer> idx_server;
	std::vector<std::unique_ptr<TestNodeServer>> nodes;
	std::vector<TP> threads;


};


#endif /* UNITTESTS_CACHE_UTIL_H_ */
