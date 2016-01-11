/*
 * util.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include "test/unittests/cache/util.h"
#include "util/sizeutil.h"
#include "util/make_unique.h"
#include <chrono>
#include <deque>


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

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg, bool allow_infinite) {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for (int i = 0; i < 4; i++)
		bbox[i] = NAN;

	// Figure out if we know the extent of the CRS
	// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
	//                               minx          miny         maxx         maxy
	double extent_webmercator[4] { -20037508.34, -20037508.34, 20037508.34, 20037508.34 };
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
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::max(extent[element], extent[(element + 2) % 4]);
		}
		else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::min(extent[element], extent[(element + 2) % 4]);
		}
		else {
			value = std::stod(stringValue);
			if (!std::isfinite(value))
				throw ArgumentException("BBOX contains entry that is not a finite number");
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
			bbox_normalized[i] = (bbox[i] - extent[0]) / (extent[2] - extent[0]);
			bbox_normalized[i + 1] = (bbox[i + 1] - extent[1]) / (extent[3] - extent[1]);
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


QueryRectangle random_rect(epsg_t epsg, double extend, double time, uint32_t res) {
	double x1, x2, y1, y2;

	SpatialReference bounds = SpatialReference::extent(epsg);
	double rx = bounds.x2 - bounds.x1 - extend;
	double ry = bounds.y2 - bounds.y1 - extend;

	x1 = drand48() * rx + bounds.x1;
	x2 = x1+extend;

	y1 = drand48() * ry + bounds.y1;
	y2 = y1+extend;

	return QueryRectangle(
		SpatialReference(epsg,x1,y1,x2,y2),
		TemporalReference(TIMETYPE_UNIX,time,time),
		(res > 0) ? QueryResolution::pixels(res,res) : QueryResolution::none()
	);
}



//
// TRACER
//

template<class T, CacheType TYPE>
TracingCacheWrapper<T,TYPE>::TracingCacheWrapper(
		std::vector<QTriple>& query_log) : query_log(query_log) {
}

template<class T, CacheType TYPE>
void TracingCacheWrapper<T,TYPE>::put(const std::string& semantic_id,
		const std::unique_ptr<T>& item, const QueryRectangle& query,
		const QueryProfiler& profiler) {
	(void) semantic_id; (void) item; (void) query; (void) profiler;
}

template<class T, CacheType TYPE>
std::unique_ptr<T> TracingCacheWrapper<T,TYPE>::query(
		const GenericOperator& op, const QueryRectangle& rect) {
	query_log.push_back( QTriple(TYPE,rect,op.getSemanticId()) );
	throw NoSuchElementException("NOP");
}


TracingCacheManager::TracingCacheManager() :
		query_log(), rw(query_log), pw(query_log), lw(query_log), pow(query_log), plw(query_log) {
}

CacheWrapper<GenericRaster>& TracingCacheManager::get_raster_cache() { return rw; }
CacheWrapper<PointCollection>& TracingCacheManager::get_point_cache() { return pw; }
CacheWrapper<LineCollection>& TracingCacheManager::get_line_cache() { return lw; }
CacheWrapper<PolygonCollection>& TracingCacheManager::get_polygon_cache() { return pow; }
CacheWrapper<GenericPlot>& TracingCacheManager::get_plot_cache() { return plw; }


void execute_operator(GenericOperator& op, const QueryRectangle& query,
		CacheType type) {
	QueryProfiler qp;
	switch ( type ) {
	case CacheType::RASTER:
		op.getCachedRaster(query,qp);
		break;
	case CacheType::POINT:
		op.getCachedPointCollection(query,qp);
		break;
	case CacheType::LINE:
		op.getCachedLineCollection(query,qp);
		break;
	case CacheType::POLYGON:
		op.getCachedPolygonCollection(query,qp);
		break;
	case CacheType::PLOT:
		op.getCachedPlot(query,qp);
		break;
	default:
		throw ArgumentException("Illegal query type");
	}
}

void execute(const QTriple& t) {
	auto op = GenericOperator::fromJSON(t.semantic_id);
	execute_operator(*op,t.query,t.type);
}


std::vector<QTriple> get_query_steps(const std::string& semantic_id,
		const QueryRectangle& query, CacheType type) {
	CacheManager *orig =nullptr;
	try {
	 orig = &CacheManager::get_instance();
	} catch ( const NotInitializedException &nie ) {
	}
	TracingCacheManager tcm;
	auto op = GenericOperator::fromJSON(semantic_id);
	CacheManager::init(&tcm);
	execute_operator(*op,query,type);
	auto res = tcm.query_log;
	CacheManager::init(orig);
	return res;
}

void execute_query_steps(const std::vector<QTriple>& queries) {
	auto iter = queries.rbegin();
	while ( iter != queries.rend() ) {
		execute(*iter);
		iter++;
	}
}


//
// Local cache manager
//

template<class T>
LocalCacheWrapper<T>::LocalCacheWrapper(NodeCache<T>& cache, std::unique_ptr<Puzzler<T> > puzzler, CachingStrategy *strategy) :
	cache(cache), retriever(cache), puzzle_util(this->retriever, std::move(puzzler)), strategy(strategy) {
}

template<class T>
void LocalCacheWrapper<T>::put(const std::string &semantic_id,
		const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler) {

	size_t size = SizeUtil::get_byte_size(*item);

	if ( strategy->do_cache(profiler,size) ) {
		CacheCube cube(*item);
		// Min/Max resolution hack
		if ( query.restype == QueryResolution::Type::PIXELS ) {
			double scale_x = (query.x2-query.x1) / query.xres;
			double scale_y = (query.y2-query.y1) / query.yres;

			// Result was max
			if ( scale_x < cube.resolution_info.pixel_scale_x.a )
				cube.resolution_info.pixel_scale_x.a = 0;
			// Result was minimal
			else if ( scale_x > cube.resolution_info.pixel_scale_x.b )
				cube.resolution_info.pixel_scale_x.b = std::numeric_limits<double>::infinity();


			// Result was max
			if ( scale_y < cube.resolution_info.pixel_scale_y.a )
				cube.resolution_info.pixel_scale_y.a = 0;
			// Result was minimal
			else if ( scale_y > cube.resolution_info.pixel_scale_y.b )
				cube.resolution_info.pixel_scale_y.b = std::numeric_limits<double>::infinity();
		}
		cache.put(semantic_id, item, CacheEntry( cube, size, strategy->get_costs(profiler,size)));
	}
}

template<class T>
std::unique_ptr<T> LocalCacheWrapper<T>::query(const GenericOperator& op,
		const QueryRectangle& rect) {

	CacheQueryResult<uint64_t> qres = cache.query(op.getSemanticId(), rect);

	// Full single local hit
	if ( !qres.has_remainder() && qres.keys.size() == 1 ) {
		NodeCacheKey key(op.getSemanticId(), qres.keys.at(0));
		return cache.get_copy(key);
	}
	// Partial or Full puzzle
	else if ( qres.has_hit() ) {
		std::vector<CacheRef> refs;
		refs.reserve(qres.keys.size());
		for ( auto &id : qres.keys )
			refs.push_back( CacheRef("testhost",12345,id) );

		PuzzleRequest pr( cache.type, op.getSemanticId(), rect, qres.remainder, refs );
		return process_puzzle(pr);
	}
	else {
		throw NoSuchElementException("MISS");
	}
}

template<class T>
std::unique_ptr<T> LocalCacheWrapper<T>::process_puzzle(const PuzzleRequest& request) {
	QueryProfiler qp;
	auto result = puzzle_util.process_puzzle( request, qp );
	put( request.semantic_id, result, request.query, qp );
	return result;
}

LocalCacheManager::LocalCacheManager(std::unique_ptr<CachingStrategy> strategy,
		size_t raster_cache_size, size_t point_cache_size,
		size_t line_cache_size, size_t polygon_cache_size,
		size_t plot_cache_size ) :
	rc(CacheType::RASTER,raster_cache_size),
	pc(CacheType::POINT,point_cache_size),
	lc(CacheType::LINE,line_cache_size),
	poc(CacheType::POLYGON,polygon_cache_size),
	plc(CacheType::PLOT,plot_cache_size),
	rw(rc, make_unique<RasterPuzzler>(), strategy.get()),
	pw(pc, make_unique<PointCollectionPuzzler>(), strategy.get()),
	lw(lc, make_unique<LineCollectionPuzzler>(), strategy.get()),
	pow(poc, make_unique<PolygonCollectionPuzzler>(), strategy.get()),
	plw(plc, make_unique<PlotPuzzler>(), strategy.get()),
	strategy(std::move(strategy)) {
}

CacheWrapper<GenericRaster>& LocalCacheManager::get_raster_cache() {
	return rw;
}

CacheWrapper<PointCollection>& LocalCacheManager::get_point_cache() {
	return pw;
}

CacheWrapper<LineCollection>& LocalCacheManager::get_line_cache() {
	return lw;
}

CacheWrapper<PolygonCollection>& LocalCacheManager::get_polygon_cache() {
	return pow;
}

CacheWrapper<GenericPlot>& LocalCacheManager::get_plot_cache() {
	return plw;
}

void LocalCacheManager::set_strategy(
		std::unique_ptr<CachingStrategy> strategy) {
	this->strategy.reset( strategy.release() );
	rw.strategy = this->strategy.get();
	pw.strategy = this->strategy.get();
	lw.strategy = this->strategy.get();
	pow.strategy = this->strategy.get();
	plw.strategy = this->strategy.get();
}

//
// Test extensions
//

TestNodeServer::TestNodeServer(int num_threads, uint32_t my_port, const std::string &index_host, uint32_t index_port, const std::string &strategy, size_t capacity)  :
	NodeServer( make_unique<NodeCacheManager>( CachingStrategy::by_name(strategy), capacity,capacity,capacity,capacity,capacity ), my_port,index_host,index_port,num_threads) {
}

bool TestNodeServer::owns_current_thread() {
	for ( auto &t : workers ) {
		if ( std::this_thread::get_id() == t->get_id() )
			return true;
	}
	return (delivery_thread != nullptr && std::this_thread::get_id() == delivery_thread->get_id()) ||
		    std::this_thread::get_id() == my_id;
}

void TestNodeServer::run_node_thread(TestNodeServer* ns) {
	ns->my_id = std::this_thread::get_id();
	ns->run();
}

NodeCacheManager& TestNodeServer::get_cache_manager() {
	return *manager;
}

// Test index

TestIdxServer::TestIdxServer(uint32_t port, const std::string &reorg_strategy)  : IndexServer(port,reorg_strategy) {
	no_updates = true;
}

void TestIdxServer::trigger_reorg(uint32_t node_id, const ReorgDescription& desc)  {
	for ( auto &cc : control_connections ) {
		if ( cc.second->node->id == node_id ) {
			cc.second->send_reorg(desc);
			return;
		}
	}
	throw ArgumentException(concat("No node found for id ", node_id));
}

void TestIdxServer::wait_for_idle_control_connections() {
	bool all_idle = true;
	// Wait until connections are idle;
	do {
		all_idle = true;
		if ( !all_idle )
			std::this_thread::sleep_for( std::chrono::milliseconds(500) );

		for ( auto &kv : control_connections ) {
			all_idle &= kv.second->get_state() == ControlState::IDLE;
		}
	} while (!all_idle);
}

void TestIdxServer::force_stat_update() {
	wait_for_idle_control_connections();

	for ( auto &kv : control_connections ) {
		kv.second->send_get_stats();
	}

	wait_for_idle_control_connections();
}

void TestIdxServer::force_reorg() {
	force_stat_update();
	reorganize(true);
	wait_for_idle_control_connections();
}

void TestIdxServer::reset_stats() {
	for ( auto &p : nodes )
		p.second->reset_query_stats();
	query_manager.reset_stats();
}

//
// Test Cache Manager
//

NodeCacheManager& TestCacheMan::get_instance_mgr(int i) {
	return *instances.at(i)->manager;
}

NodeCacheWrapper<GenericRaster>& TestCacheMan::get_raster_cache() {
	return get_current_instance().get_raster_cache();
}

NodeCacheWrapper<PointCollection>& TestCacheMan::get_point_cache() {
	return get_current_instance().get_point_cache();
}

NodeCacheWrapper<LineCollection>& TestCacheMan::get_line_cache() {
	return get_current_instance().get_line_cache();
}

NodeCacheWrapper<PolygonCollection>& TestCacheMan::get_polygon_cache() {
	return get_current_instance().get_polygon_cache();
}

NodeCacheWrapper<GenericPlot>& TestCacheMan::get_plot_cache() {
	return get_current_instance().get_plot_cache();
}

void TestCacheMan::add_instance(TestNodeServer *inst) {
	instances.push_back(inst);
}

NodeCacheManager& TestCacheMan::get_current_instance() const {
	for (auto i : instances) {
		if (i->owns_current_thread())
			return *i->manager;
	}
	throw ArgumentException("Unregistered instance called cache-manager");
}




const int LocalTestSetup::INDEX_PORT;

LocalTestSetup::LocalTestSetup(int num_nodes, int num_workers,
		size_t capacity, std::string reorg_strat, std::string c_strat ) :
		ccm("localhost",INDEX_PORT), idx_server(make_unique<TestIdxServer>(INDEX_PORT,reorg_strat)) {

	for ( int i = 1; i <= num_nodes; i++)
		nodes.push_back( make_unique<TestNodeServer>(num_workers, INDEX_PORT+i,"localhost",INDEX_PORT,c_strat,capacity) );

	for ( auto &n : nodes )
		mgr.add_instance(n.get());
	CacheManager::init(&mgr);

	threads.push_back( make_unique<std::thread>(&IndexServer::run, idx_server.get()) );
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));

	for ( auto &n : nodes )
		threads.push_back( make_unique<std::thread>(TestNodeServer::run_node_thread, n.get()));
}

LocalTestSetup::~LocalTestSetup() {

	for ( auto &n : nodes )
		n->stop();

	idx_server->stop();

	for (TP &t : threads )
		t->join();
}

ClientCacheManager& LocalTestSetup::get_client() {
	return ccm;
}

TestIdxServer& LocalTestSetup::get_index() {
	return *idx_server;
}

std::vector<std::unique_ptr<TestNodeServer> >& LocalTestSetup::get_nodes() {
	return nodes;
}

