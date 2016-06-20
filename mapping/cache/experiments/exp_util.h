/*
 * exp_util.h
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#ifndef EXPERIMENTS_EXP_UTIL_H_
#define EXPERIMENTS_EXP_UTIL_H_

#include "cache/index/indexserver.h"
#include "cache/node/node_manager.h"
#include "cache/node/nodeserver.h"
#include "cache/manager.h"
#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"
#include "util/configuration.h"

#include <iostream>
#include <vector>
#include <string>
#include <cmath>
#include <thread>
#include <memory>
#include <random>

time_t parseIso8601DateTime(std::string dateTimeString);

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg =
		EPSG_WEBMERCATOR, bool allow_infinite = false);


//
// Test classes
//

class TestIdxServer: public IndexServer {
public:
	TestIdxServer(uint32_t port, time_t update_interval,
			const std::string &reorg_strategy,
			const std::string &relevance_function,
			const std::string &scheduler,
			bool batching);
	~TestIdxServer();
	void trigger_reorg(uint32_t node_id, const ReorgDescription &desc);
	void force_stat_update();
	void force_reorg();
	void reset_stats();
	SystemStats get_stats();
private:
	void wait_for_idle_control_connections();
};

class TestNodeServer: public NodeServer {
	static std::unique_ptr<NodeCacheManager> get_mgr( const std::string &cache_mgr, const std::string &strategy, const std::string &local_repl, size_t capacity );
public:
	static void run_node_thread(TestNodeServer *ns);


	TestNodeServer(int num_threads, uint32_t my_port,
			const std::string &index_host, uint32_t index_port,
			const std::string &strategy, const std::string &cache_mgr, const std::string &local_repl, size_t capacity = 5 * 1024 * 1024);
	NodeCacheManager &get_cache_manager();
	uint32_t get_id() const {return NodeServer::my_id;};
	uint32_t get_port() const {return NodeServer::my_port;};
	const std::string& get_host() const {return NodeServer::my_host;};

	bool owns_current_thread();
private:
	std::thread::id my_thread_id;
};

template<typename T>
class TestCacheWrapper: public CacheWrapper<T> {
public:
	TestCacheWrapper(NodeCacheWrapper<T> &w, ProfilingData &costs) :
			w(w), costs(costs) {
	}
	;

	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item,
			const QueryRectangle &query, const QueryProfiler &profiler) {
		costs.all_cpu += profiler.self_cpu;
		costs.all_gpu += profiler.self_gpu;
		costs.all_io += profiler.self_io;
		return w.put(semantic_id, item, query, profiler);
	}

	std::unique_ptr<T> query(GenericOperator &op,
			const QueryRectangle &rect, QueryProfiler &profiler) {
		return w.query(op, rect, profiler);
	}
	;

private:
	NodeCacheWrapper<T> &w;
	ProfilingData &costs;
};

class TestCacheMan: public CacheManager {
public:
	void add_instance(TestNodeServer *inst);
	NodeCacheManager& get_instance_mgr(int i);
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
	ProfilingData &get_costs();
	void reset_costs();
private:
	std::map<TestNodeServer*, TestCacheWrapper<GenericRaster>> raster_wrapper;
	NodeCacheManager& get_current_instance() const;
	std::vector<TestNodeServer*> instances;
	ProfilingData costs;
};

class LocalCacheManager;

class LocalTestSetup {
public:
	LocalTestSetup(int num_nodes, int num_workers, time_t update_interval,
			size_t capacity, std::string reorg_strat,
			std::string relevance_function,
			std::string c_strat,
			std::string scheduler = "default",
			bool batching = true,
			std::string node_cache = "remote",
			std::string node_repl = "lru",
			int index_port = atoi(
					Configuration::get("indexserver.port").c_str()));
	~LocalTestSetup();
	ClientCacheManager &get_client();
	TestIdxServer &get_index();
	TestCacheMan &get_manager();
	TestNodeServer& get_node(uint32_t id);

	std::vector<std::unique_ptr<TestNodeServer>>& get_nodes();
private:
	int index_port;
	TestCacheMan mgr;
	ClientCacheManager ccm;
	std::unique_ptr<TestIdxServer> idx_server;
	std::vector<std::unique_ptr<TestNodeServer>> nodes;
	std::vector<std::unique_ptr<std::thread>> threads;
};

//
// TRACER
//

class QTriple {
public:
	QTriple(CacheType type, const QueryRectangle &query,
			const std::string &semantic_id);
	QTriple();
	QTriple& operator=(const QTriple &t);
	CacheType type;
	QueryRectangle query;
	std::string semantic_id;
};

class QuerySpec {
private:
	static std::default_random_engine generator;
	static std::uniform_real_distribution<double> distrib;
public:
	QuerySpec(const std::string &workflow, epsg_t epsg, CacheType type,
			const TemporalReference &tref, std::string name = "");
	std::string workflow;
	epsg_t epsg;
	CacheType type;
	TemporalReference tref;
	std::string name;
	SpatialReference bounds;
	QueryRectangle random_rectangle(double extend,
			uint32_t resolution = 0) const;
	QueryRectangle random_rectangle_percent(double percent,
			uint32_t resolution = 0) const;
	std::vector<QueryRectangle> disjunct_rectangles(size_t num, double extend,
			uint32_t resolution) const;
	std::vector<QueryRectangle> disjunct_rectangles_percent(size_t num,
			double percent, uint32_t resolution) const;
	QueryRectangle rectangle(double x1, double y1, double extend,
			uint32_t resolution = 0) const;
	size_t get_num_operators() const;
	std::vector<QTriple> guess_query_steps(const QueryRectangle &rect) const;
private:
	size_t get_num_operators(GenericOperator *op) const;
	void get_op_spec(GenericOperator* op, QueryRectangle rect,
			std::vector<QTriple> &result) const;
};

class ParallelExecutor {
	friend class std::thread;
public:
	ParallelExecutor(const std::deque<QTriple> &queries,
			ClientCacheManager &mgr, int num_threads);
	void execute();
private:
	void thread_exec();
	std::vector<std::unique_ptr<std::thread>> threads;
	std::deque<QTriple> queries;
	ClientCacheManager &mgr;
	int num_threads;
	std::mutex mtx;
};

template<class T, CacheType TYPE>
class TracingCacheWrapper: public CacheWrapper<T> {
public:
	TracingCacheWrapper(std::vector<QTriple> &query_log, size_t &size);
	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item,
			const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(GenericOperator &op,
			const QueryRectangle &rect, QueryProfiler &profiler);
private:
	size_t &size;
	std::vector<QTriple> &query_log;
};

class TracingCacheManager: public CacheManager {
public:
	TracingCacheManager();
	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
	size_t size;
	std::vector<QTriple> query_log;
private:
	TracingCacheWrapper<GenericRaster, CacheType::RASTER> rw;
	TracingCacheWrapper<PointCollection, CacheType::POINT> pw;
	TracingCacheWrapper<LineCollection, CacheType::LINE> lw;
	TracingCacheWrapper<PolygonCollection, CacheType::POLYGON> pow;
	TracingCacheWrapper<GenericPlot, CacheType::PLOT> plw;
};

//////////////////////////////////////////////////
//
// EXPERIMENTS
//
//////////////////////////////////////////////////

class CacheExperiment {
public:
	typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
	typedef std::chrono::system_clock SysClock;
	static size_t duration(const TimePoint &start, const TimePoint &end);
	static void execute_query(GenericOperator &op, const QueryRectangle &query,
			CacheType type, QueryProfiler &qp);
	static void execute_query(const QTriple &query, QueryProfiler &qp);
	static void execute_queries(const std::vector<QTriple> &queries,
			QueryProfiler &qp);
	static void execute_query(ClientCacheManager &mgr, const QTriple &t);
public:
	CacheExperiment(const std::string &name, uint32_t num_runs);
	virtual ~CacheExperiment() = default;
	void run();
protected:
	virtual void global_setup();
	virtual void global_teardown();
	virtual void setup();
	virtual void teardown();
	virtual void print_results() = 0;
	virtual void run_once() = 0;
public:
	const std::string name;
	const uint32_t num_runs;
};

class CacheExperimentSingleQuery: public CacheExperiment {
public:
	CacheExperimentSingleQuery(const std::string &name, const QuerySpec &spec,
			uint32_t num_runs);
	virtual ~CacheExperimentSingleQuery() = default;
public:
	const QuerySpec query_spec;
};

class CacheExperimentMultiQuery: public CacheExperiment {
public:
	CacheExperimentMultiQuery(const std::string &name,
			const std::vector<QuerySpec> &specs, uint32_t num_runs);
	virtual ~CacheExperimentMultiQuery() = default;
public:
	const std::vector<QuerySpec> query_specs;
};

#endif /* EXPERIMENTS_EXP_UTIL_H_ */
