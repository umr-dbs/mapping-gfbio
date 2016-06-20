/*
 * exp_util.cpp
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#include "cache/experiments/exp_util.h"
#include "cache/experiments/cheat.h"
#include "cache/priv/connection.h"
#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include "util/sizeutil.h"
#include "util/make_unique.h"
#include "util/gdal.h"

#include <chrono>
#include <deque>
#include <algorithm>

time_t parseIso8601DateTime(std::string dateTimeString) {
	const std::string dateTimeFormat { "%Y-%m-%dT%H:%M:%S" }; //TODO: we should allow millisec -> "%Y-%m-%dT%H:%M:%S.SSSZ" std::get_time and the tm struct dont have them.

	//std::stringstream dateTimeStream{dateTimeString}; //TODO: use this with gcc >5.0
	tm queryDateTime;
	//std::get_time(&queryDateTime, dateTimeFormat); //TODO: use this with gcc >5.0
	strptime(dateTimeString.c_str(), dateTimeFormat.c_str(), &queryDateTime); //TODO: remove this with gcc >5.0
	time_t queryTimestamp = timegm(&queryDateTime); //TODO: is there a c++ version for timegm?

	//TODO: parse millisec

	return (queryTimestamp);
}

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg,
		bool allow_infinite) {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for (int i = 0; i < 4; i++)
		bbox[i] = NAN;

	// Figure out if we know the extent of the CRS
	// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
	//                               minx          miny         maxx         maxy
	double extent_webmercator[4] { -20037508.34, -20037508.34, 20037508.34,
			20037508.34 };
	double extent_latlon[4] { -180, -90, 180, 90 };
	double extent_msg[4] { -5568748.276, -5568748.276, 5568748.276, 5568748.276 };

	double *extent = nullptr;
	if (epsg == EPSG_WEBMERCATOR)
		extent = extent_webmercator;
	else if (epsg == EPSG_LATLON)
		extent = extent_latlon;
	else if (epsg == EPSG_GEOSMSG)
		extent = extent_msg;

	std::string delimiters = " ,";
	size_t current, next = -1;
	int element = 0;
	do {
		current = next + 1;
		next = bbox_str.find_first_of(delimiters, current);
		std::string stringValue = bbox_str.substr(current, next - current);
		double value = 0;

		if (stringValue == "Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException(
						"cannot process BBOX with Infinity and unknown CRS");
			value = std::max(extent[element], extent[(element + 2) % 4]);
		} else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException(
						"cannot process BBOX with Infinity and unknown CRS");
			value = std::min(extent[element], extent[(element + 2) % 4]);
		} else {
			value = std::stod(stringValue);
			if (!std::isfinite(value))
				throw ArgumentException(
						"BBOX contains entry that is not a finite number");
		}

		bbox[element++] = value;
	} while (element < 4 && next != std::string::npos);

	if (element != 4)
		throw ArgumentException("Could not parse BBOX parameter");

	/*
	 * OpenLayers insists on sending latitude in x and longitude in y.
	 * The MAPPING code (including gdal's projection classes) don't agree: east/west should be in x.
	 * The simple solution is to swap the x and y coordinates.
	 * OpenLayers 3 uses the axis orientation of the projection to determine the bbox axis order. https://github.com/openlayers/ol3/blob/master/src/ol/source/imagewmssource.js ~ line 317.
	 */
	if (epsg == EPSG_LATLON) {
		std::swap(bbox[0], bbox[1]);
		std::swap(bbox[2], bbox[3]);
	}

	// If no extent is known, just trust the client.
	if (extent) {
		double bbox_normalized[4];
		for (int i = 0; i < 4; i += 2) {
			bbox_normalized[i] = (bbox[i] - extent[0])
					/ (extent[2] - extent[0]);
			bbox_normalized[i + 1] = (bbox[i + 1] - extent[1])
					/ (extent[3] - extent[1]);
		}

		// Koordinaten können leicht ausserhalb liegen, z.B.
		// 20037508.342789, 20037508.342789
		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 && bbox_normalized[i] > -0.001)
				bbox_normalized[i] = 0.0;
			else if (bbox_normalized[i] > 1.0 && bbox_normalized[i] < 1.001)
				bbox_normalized[i] = 1.0;
		}

		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0)
				throw ArgumentException("BBOX exceeds extent");
		}
	}

	//bbox_normalized[1] = 1.0 - bbox_normalized[1];
	//bbox_normalized[3] = 1.0 - bbox_normalized[3];
}

//
// Test extensions
//

std::unique_ptr<NodeCacheManager> TestNodeServer::get_mgr(
		const std::string& cache_mgr, const std::string& strategy,
		const std::string& local_repl, size_t capacity) {

	return NodeCacheManager::by_name(cache_mgr, capacity, capacity, capacity,
			capacity, capacity, strategy, local_repl);
}

TestNodeServer::TestNodeServer(int num_threads, uint32_t my_port,
		const std::string &index_host, uint32_t index_port,
		const std::string &strategy, const std::string &cache_mgr,
		const std::string &local_repl, size_t capacity) :
		NodeServer(get_mgr(cache_mgr, strategy, local_repl, capacity), my_port,
				index_host, index_port, num_threads) {

}

bool TestNodeServer::owns_current_thread() {
	for (auto &t : workers) {
		if (std::this_thread::get_id() == t->get_id())
			return true;
	}
	return (delivery_thread != nullptr
			&& std::this_thread::get_id() == delivery_thread->get_id())
			|| std::this_thread::get_id() == my_thread_id;
}

void TestNodeServer::run_node_thread(TestNodeServer* ns) {
	ns->my_thread_id = std::this_thread::get_id();
	ns->run();
}

NodeCacheManager& TestNodeServer::get_cache_manager() {
	return *manager;
}

// Test index

TestIdxServer::TestIdxServer(uint32_t port, time_t update_interval,
		const std::string &reorg_strategy,
		const std::string &relevance_function, const std::string &scheduler, bool batching) :
		IndexServer(port, update_interval, reorg_strategy, relevance_function, batching,
				scheduler) {
}

void TestIdxServer::trigger_reorg(uint32_t node_id,
		const ReorgDescription& desc) {
	Log::info("Triggering reorg");
	for ( auto &n : nodes ) {
		if ( n.first == node_id ) {
			n.second->send_reorg(desc);
			wakeup();
			return;
		}
	}
	throw ArgumentException(concat("No node found for id ", node_id));
}

TestIdxServer::~TestIdxServer() {
	QueryStats cumulated;
	for (auto &p : nodes) {
		Node &n = *p.second;
		cumulated += n.get_query_stats();
	}
	cumulated += query_manager->get_stats();
	std::cout << "Cumulated " << cumulated.to_string() << std::endl;
	std::cout << query_manager->get_stats().to_string() << std::endl;

}

void TestIdxServer::wait_for_idle_control_connections() {
	bool all_idle = true;
	// Wait until connections are idle;
	do {
		if (!all_idle)
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		all_idle = true;
		for (auto &kv : nodes) {
			all_idle &= kv.second->is_control_connection_idle();
		}
	} while (!all_idle);
}

void TestIdxServer::force_stat_update() {
	wait_for_idle_control_connections();

	for (auto &kv : nodes) {
		kv.second->send_stats_request();
	}
	wakeup();
	wait_for_idle_control_connections();
}

void TestIdxServer::force_reorg() {
	force_stat_update();
	reorganize(true);
	wakeup();
	wait_for_idle_control_connections();
}

void TestIdxServer::reset_stats() {
	for (auto &p : nodes)
		p.second->reset_query_stats();
	query_manager->reset_stats();
}

SystemStats TestIdxServer::get_stats() {
	return query_manager->get_stats();
}

//
// Test Cache Manager
//

NodeCacheManager& TestCacheMan::get_instance_mgr(int i) {
	return *instances.at(i)->manager;
}

CacheWrapper<GenericRaster>& TestCacheMan::get_raster_cache() {
	for (auto i : instances)
		if (i->owns_current_thread())
			return raster_wrapper.at(i);
	throw ArgumentException("Unregistered instance called cache-manager");
}

CacheWrapper<PointCollection>& TestCacheMan::get_point_cache() {
	return get_current_instance().get_point_cache();
}

CacheWrapper<LineCollection>& TestCacheMan::get_line_cache() {
	return get_current_instance().get_line_cache();
}

CacheWrapper<PolygonCollection>& TestCacheMan::get_polygon_cache() {
	return get_current_instance().get_polygon_cache();
}

CacheWrapper<GenericPlot>& TestCacheMan::get_plot_cache() {
	return get_current_instance().get_plot_cache();
}

void TestCacheMan::add_instance(TestNodeServer *inst) {
	instances.push_back(inst);
	raster_wrapper.emplace(inst,
			TestCacheWrapper<GenericRaster>(inst->manager->get_raster_cache(),
					costs));
}

NodeCacheManager& TestCacheMan::get_current_instance() const {
	for (auto i : instances) {
		if (i->owns_current_thread())
			return *i->manager;
	}
	throw ArgumentException("Unregistered instance called cache-manager");
}

void TestCacheMan::reset_costs() {
	costs.all_cpu = 0;
	costs.all_gpu = 0;
	costs.all_io = 0;
	costs.self_cpu = 0;
	costs.self_gpu = 0;
	costs.self_io = 0;
	costs.uncached_cpu = 0;
	costs.uncached_gpu = 0;
	costs.uncached_io = 0;
}

LocalTestSetup::LocalTestSetup(int num_nodes, int num_workers,
		time_t update_interval, size_t capacity, std::string reorg_strat,
		std::string relevance_function, std::string c_strat,
		std::string scheduler, bool batching, std::string node_cache, std::string node_repl,
		int index_port) :

		index_port(index_port), ccm("127.0.0.1", index_port), idx_server(
				make_unique<TestIdxServer>(index_port, update_interval,
						reorg_strat, relevance_function, scheduler, batching)) {

	for (int i = 1; i <= num_nodes; i++)
		nodes.push_back(
				make_unique<TestNodeServer>(num_workers, index_port + i,
						"127.0.0.1", index_port, c_strat, node_cache, node_repl,
						capacity));

	for (auto &n : nodes)
		mgr.add_instance(n.get());
	CacheManager::init(&mgr);

	threads.push_back(
			make_unique<std::thread>(&IndexServer::run, idx_server.get()));
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	for (auto &n : nodes)
		threads.push_back(
				make_unique<std::thread>(TestNodeServer::run_node_thread,
						n.get()));
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

LocalTestSetup::~LocalTestSetup() {

	for (auto &n : nodes)
		n->stop();

	for (size_t i = 1; i < threads.size(); i++)
		threads[i]->join();

	idx_server->stop();

	threads[0]->join();
	Log::warn("Test-Setup done!");
}

ClientCacheManager& LocalTestSetup::get_client() {
	return ccm;
}

TestIdxServer& LocalTestSetup::get_index() {
	return *idx_server;
}

TestNodeServer& LocalTestSetup::get_node(uint32_t id) {
	for (auto &n : nodes)
		if (n->get_id() == id)
			return *n;
	throw ArgumentException(concat("No node with id: ", id));
}

std::vector<std::unique_ptr<TestNodeServer> >& LocalTestSetup::get_nodes() {
	return nodes;
}

TestCacheMan& LocalTestSetup::get_manager() {
	return mgr;
}

ProfilingData& TestCacheMan::get_costs() {
	return costs;
}

template<class T, CacheType TYPE>
TracingCacheWrapper<T, TYPE>::TracingCacheWrapper(
		std::vector<QTriple>& query_log, size_t &size) :
		size(size), query_log(query_log) {
}

template<class T, CacheType TYPE>
bool TracingCacheWrapper<T, TYPE>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle& query,
		const QueryProfiler& profiler) {
	(void) semantic_id;
	(void) item;
	(void) query;
	(void) profiler;
	query_log.push_back(QTriple(TYPE, query, semantic_id));
	size += SizeUtil::get_byte_size(*item);
	return false;
}

template<class T, CacheType TYPE>
std::unique_ptr<T> TracingCacheWrapper<T, TYPE>::query(GenericOperator& op,
		const QueryRectangle& rect, QueryProfiler &profiler) {
	(void) op;
	(void) rect;
	(void) profiler;
	throw NoSuchElementException("NOP");
}

TracingCacheManager::TracingCacheManager() :
		size(0), query_log(), rw(query_log, size), pw(query_log, size), lw(
				query_log, size), pow(query_log, size), plw(query_log, size) {
}

CacheWrapper<GenericRaster>& TracingCacheManager::get_raster_cache() {
	return rw;
}
CacheWrapper<PointCollection>& TracingCacheManager::get_point_cache() {
	return pw;
}
CacheWrapper<LineCollection>& TracingCacheManager::get_line_cache() {
	return lw;
}
CacheWrapper<PolygonCollection>& TracingCacheManager::get_polygon_cache() {
	return pow;
}
CacheWrapper<GenericPlot>& TracingCacheManager::get_plot_cache() {
	return plw;
}

ParallelExecutor::ParallelExecutor(const std::deque<QTriple>& queries,
		ClientCacheManager& mgr, int num_threads) :
		queries(queries), mgr(mgr), num_threads(num_threads) {
}

void ParallelExecutor::execute() {
	{
		std::lock_guard<std::mutex> lock(mtx);
		for (int i = 0; i < num_threads; i++) {
			threads.push_back(
					make_unique<std::thread>(&ParallelExecutor::thread_exec,
							this));
		}
	}

	for (auto &tp : threads)
		tp->join();
}

void ParallelExecutor::thread_exec() {
	while (true) {
		QTriple qt;
		{
			std::lock_guard<std::mutex> lock(mtx);
			if (queries.empty())
				return;
			qt = queries.front();
			queries.pop_front();
		}
		CacheExperiment::execute_query(mgr, qt);
	}
}

//
// Query Stuff
//

QTriple::QTriple() :
		type(CacheType::UNKNOWN), query(SpatialReference::unreferenced(),
				TemporalReference::unreferenced(), QueryResolution::none()) {
}

QTriple::QTriple(CacheType type, const QueryRectangle& query,
		const std::string& semantic_id) :
		type(type), query(query), semantic_id(semantic_id) {
}

QTriple& QTriple::operator =(const QTriple& t) {
	type = t.type;
	query = t.query;
	semantic_id = t.semantic_id;
	return *this;
}

//
// SPEC
//

std::default_random_engine QuerySpec::generator(
		std::chrono::system_clock::now().time_since_epoch().count());
std::uniform_real_distribution<double> QuerySpec::distrib(0, 1);

QuerySpec::QuerySpec(const std::string& workflow, epsg_t epsg, CacheType type,
		const TemporalReference& tref, std::string name) :
		workflow(workflow), epsg(epsg), type(type), tref(tref), name(name), bounds(
				SpatialReference::extent(epsg)) {
}

QueryRectangle QuerySpec::rectangle(double x1, double y1, double extend,
		uint32_t resolution) const {
	return QueryRectangle(
			SpatialReference(epsg, x1, y1, x1 + extend, y1 + extend), tref,
			type == CacheType::RASTER ?
					QueryResolution::pixels(resolution, resolution) :
					QueryResolution::none());
}

QueryRectangle QuerySpec::random_rectangle_percent(double p,
		uint32_t resolution) const {
	return random_rectangle((bounds.x2 - bounds.x1) * p, resolution);
}

QueryRectangle QuerySpec::random_rectangle(double extend,
		uint32_t resolution) const {
	double rx = bounds.x2 - bounds.x1 - extend;
	double ry = bounds.y2 - bounds.y1 - extend;
	double x1, y1;

	x1 = distrib(generator) * rx + bounds.x1;
	y1 = distrib(generator) * ry + bounds.y1;
	return rectangle(x1, y1, extend, resolution);
}

std::vector<QueryRectangle> QuerySpec::disjunct_rectangles(size_t num,
		double extend, uint32_t resolution) const {

	size_t guard = 0;

	// Create disjunct rectangles
	std::vector<QueryRectangle> rects;
	while (rects.size() < num && guard < 10000) {
		auto rect = random_rectangle(extend, resolution);
		bool add = true;
		for (auto &r : rects) {
			add &= (rect.x2 < r.x1 || rect.x1 > r.x2)
					&& (rect.y2 < r.y1 || rect.y1 > r.y2);
		}
		if (add) {
			guard = 0;
			rects.push_back(rect);
		} else
			guard++;
	}
	if (rects.size() < num)
		throw OperatorException("Impossible to create disjunct rectangles");

	return rects;
}

std::vector<QueryRectangle> QuerySpec::disjunct_rectangles_percent(size_t num,
		double percent, uint32_t resolution) const {
	return disjunct_rectangles(num, (bounds.x2 - bounds.x1) * percent,
			resolution);
}

size_t QuerySpec::get_num_operators() const {
	auto op = GenericOperator::fromJSON(workflow);
	return get_num_operators(op.get());
}

size_t QuerySpec::get_num_operators(GenericOperator *op) const {
	size_t res = 1;
	for (int i = 0; i < op->MAX_SOURCES; i++) {
		if (op->sources[i])
			res += get_num_operators(op->sources[i]);
	}
	return res;
}

std::vector<QTriple> QuerySpec::guess_query_steps(
		const QueryRectangle& rect) const {
	std::vector<QTriple> result;
	auto op = GenericOperator::fromJSON(workflow);
	result.push_back(QTriple(type, rect, op->semantic_id));
	get_op_spec(op.get(), rect, result);
	std::reverse(result.begin(), result.end());
	return result;
}

void QuerySpec::get_op_spec(GenericOperator* op, QueryRectangle rect,
		std::vector<QTriple> &result) const {
	int offset = 0;
	CacheType type[] = { CacheType::RASTER, CacheType::POINT, CacheType::LINE,
			CacheType::POLYGON };

	if (op->type == "projection") {
		auto casted = dynamic_cast<ProjectionOperator*>(op);
		GDAL::CRSTransformer transformer(casted->dest_epsg, casted->src_epsg);
		QueryRectangle projected = casted->projectQueryRectangle(rect,
				transformer);
		rect = projected;
	} else if (op->type == "timeShiftOperator") {
		auto casted = dynamic_cast<TimeShiftOperator*>(op);
		TimeModification time_modification = casted->createTimeModification(
				rect);
		auto shifted = time_modification.apply(rect);
		rect = QueryRectangle(rect, shifted, rect);
	}

	for (int i = 0; i < GenericOperator::MAX_INPUT_TYPES; i++) {
		for (int j = 0; j < op->sourcecounts[i]; j++) {
			result.push_back(
					QTriple(type[i], rect,
							op->sources[offset + j]->semantic_id));
			get_op_spec(op->sources[offset + j], rect, result);
		}
		offset += op->sourcecounts[i];
	}
}

//
// Experiments
//

void CacheExperiment::execute_query(GenericOperator& op,
		const QueryRectangle& query, CacheType type, QueryProfiler &qp) {
	switch (type) {
	case CacheType::RASTER: {
		op.getCachedRaster(query, qp);
		break;
	}
	case CacheType::POINT:
		op.getCachedPointCollection(query, qp);
		break;
	case CacheType::LINE:
		op.getCachedLineCollection(query, qp);
		break;
	case CacheType::POLYGON:
		op.getCachedPolygonCollection(query, qp);
		break;
	case CacheType::PLOT:
		op.getCachedPlot(query, qp);
		break;
	default:
		throw ArgumentException("Illegal query type");
	}
}

void CacheExperiment::execute_query(const QTriple& query, QueryProfiler &qp) {
	auto op = GenericOperator::fromJSON(query.semantic_id);
	execute_query(*op, query.query, query.type, qp);
}

void CacheExperiment::execute_query(ClientCacheManager& mgr, const QTriple& t) {
	QueryProfiler qp;
	auto op = GenericOperator::fromJSON(t.semantic_id);
	switch (t.type) {
	case CacheType::RASTER:
		mgr.get_raster_cache().query(*op, t.query, qp);
		break;
	case CacheType::POINT:
		mgr.get_point_cache().query(*op, t.query, qp);
		break;
	case CacheType::LINE:
		mgr.get_line_cache().query(*op, t.query, qp);
		break;
	case CacheType::POLYGON:
		mgr.get_polygon_cache().query(*op, t.query, qp);
		break;
	case CacheType::PLOT:
		mgr.get_plot_cache().query(*op, t.query, qp);
		break;
	default:
		throw ArgumentException("Illegal query type");
	}
}

void CacheExperiment::execute_queries(const std::vector<QTriple>& queries,
		QueryProfiler &qp) {
	for (auto &q : queries)
		execute_query(q, qp);
}

size_t CacheExperiment::duration(const TimePoint& start, const TimePoint& end) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

CacheExperiment::CacheExperiment(const std::string& name, uint32_t num_runs) :
		name(name), num_runs(num_runs) {
}

void CacheExperiment::run() {
	std::cout << "Running experiment " << name << " (" << num_runs << " times)"
			<< std::endl;
	TimePoint start = SysClock::now();
	std::cout << "Setting up environment..." << std::flush;
	global_setup();
	std::cout << " done" << std::endl;
	for (uint32_t i = 1; i <= num_runs; i++) {
		std::cout << "Executing run " << i << "/" << num_runs << "..."
				<< std::flush;
		setup();
		run_once();
		teardown();
		std::cout << " done" << std::endl;
	}
	std::cout << "Results: " << std::endl;

	print_results();

	std::cout << "Tearing down environment...";
	global_teardown();
	std::cout << " done" << std::endl;

	TimePoint end = SysClock::now();
	std::cout << "Finished experiment " << name << ". Total execution time: "
			<< duration(start, end) << "ms" << std::endl;
	std::cout
			<< "=============================================================="
			<< std::endl;
}

void CacheExperiment::global_setup() {
}

void CacheExperiment::global_teardown() {
}

void CacheExperiment::setup() {
}

void CacheExperiment::teardown() {
}

CacheExperimentSingleQuery::CacheExperimentSingleQuery(const std::string& name,
		const QuerySpec& spec, uint32_t num_runs) :
		CacheExperiment(name + " - " + spec.name, num_runs), query_spec(spec) {
}

CacheExperimentMultiQuery::CacheExperimentMultiQuery(const std::string& name,
		const std::vector<QuerySpec>& specs, uint32_t num_runs) :
		CacheExperiment(concat(name, " - ", specs.size(), " queries"),
				num_runs), query_specs(specs) {
}
