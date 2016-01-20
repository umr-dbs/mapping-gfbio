/*
 * cache_experiment.cpp
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#include "cache/experiments/cache_experiments.h"
#include "util/nio.h"
#include <random>


////////////////////////////////////////////////////////
//
// Local cache experiment
//
////////////////////////////////////////////////////////

LocalCacheExperiment::LocalCacheExperiment(const QuerySpec& spec, uint32_t num_runs, double p, uint32_t r) :
	CacheExperimentSingleQuery("Cache-Performance", spec, num_runs), percentage(p), query_resolution(r), capacity(0),
	uncached_accum(0), cached_accum(0) {
}

void LocalCacheExperiment::global_setup() {
	capacity = 10*1024*1024;
	auto qr = query_spec.random_rectangle_percent(percentage,query_resolution);
	queries = query_spec.get_query_steps(qr);
	uncached_accum.clear();
	cached_accum.clear();
	uncached_accum.resize(queries.size(),0);
	cached_accum.resize(queries.size(),0);

}

void LocalCacheExperiment::setup() {
	auto qr = query_spec.random_rectangle_percent(percentage,query_resolution);
	queries = query_spec.get_query_steps(qr);
}

void LocalCacheExperiment::print_results() {

	size_t total = 0;
	std::cout << "Uncached results:" << std::endl;
	for ( size_t i = 0; i < queries.size(); i++ ) {
		std::cout << "  Step " << (i+1) << ": " << (uncached_accum[i]/num_runs) << "ms" << std::endl;
		total += uncached_accum[i];
	}
	std::cout << "  Total execution time: " << (total/num_runs) << std::endl;

	total = 0;
	std::cout << "Cached results:" << std::endl;
	for ( size_t i = 0; i < queries.size(); i++ ) {
		std::cout << "  Step " << (i+1) << ": " << (cached_accum[i]/num_runs) << "ms" << std::endl;
		total += cached_accum[i];
	}
	std::cout << "  Total execution time: " << (total/num_runs) << std::endl;
}

void LocalCacheExperiment::run_once() {
	// Uncached
	NopCacheManager ncm;
	execute(&ncm, uncached_accum);
	// Cached
	LocalCacheManager lcm(CachingStrategy::by_name("always"),capacity, capacity, capacity, capacity, capacity);
	execute(&lcm, cached_accum);
}

void LocalCacheExperiment::execute(CacheManager* mgr, std::vector<size_t>& accum) {
	CacheManager::init(mgr);
	TimePoint start, end;

	QueryProfiler qp;

	for ( int i = queries.size()-1; i >= 0; i-- ) {
		start = SysClock::now();
		execute_query(queries[i],qp);
		end = SysClock::now();
		accum[queries.size()-1-i] += duration(start,end);
	}
}

////////////////////////////////////////////////////////
//
// Puzzle experiment
//
////////////////////////////////////////////////////////

PuzzleExperiment::PuzzleExperiment(const QuerySpec& spec, uint32_t num_runs, double p, uint32_t r) :
		CacheExperimentSingleQuery("Puzzle-Performance", spec, num_runs), percentage(p), query_resolution(r), capacity(0) {
}

void PuzzleExperiment::global_setup() {
	capacity = 10*1024*1024;
	for ( int i = 0; i < 4; i++ ) {
		accum[i] = 0;
	}

}

void PuzzleExperiment::setup() {
	queries.clear();
	auto bounds = SpatialReference::extent(query_spec.epsg);
	double extend = (bounds.x2 - bounds.x1) * percentage;
	auto qr = query_spec.random_rectangle(extend,query_resolution);

	while ( qr.x2 + extend > bounds.x2 || qr.x1-extend < bounds.x1 || qr.y2 + extend > bounds.y2 )
		qr = query_spec.random_rectangle(extend,query_resolution);

	double d = (qr.x2-qr.x1) / 4;

	queries.push_back( QTriple(query_spec.type, qr, query_spec.workflow ) );
	// 3/4 overlap
	queries.push_back( QTriple(query_spec.type, QueryRectangle(
		SpatialReference( query_spec.epsg, qr.x1 + d, qr.y1, qr.x2 + d, qr.y2 ), qr, qr ),
		query_spec.workflow ) );

	// 1/2 overlap
	queries.push_back( QTriple(query_spec.type, QueryRectangle(
		SpatialReference( query_spec.epsg, qr.x1 - 2*d, qr.y1, qr.x2 - 2*d, qr.y2 ), qr, qr ),
		query_spec.workflow ) );

	// 1/4 overlap
	queries.push_back( QTriple(query_spec.type, QueryRectangle(
			SpatialReference( query_spec.epsg, qr.x1, qr.y1+3*d, qr.x2, qr.y2+3*d ), qr, qr ),
			query_spec.workflow ) );
}

void PuzzleExperiment::print_results() {
	std::cout << "Average execution time full query : " << (accum[0]/num_runs) << "ms" << std::endl;
	std::cout << "Average execution time 3/4 overlap: " << (accum[1]/num_runs) << "ms" << std::endl;
	std::cout << "Average execution time 1/2 overlap: " << (accum[2]/num_runs) << "ms" << std::endl;
	std::cout << "Average execution time 1/4 overlap: " << (accum[3]/num_runs) << "ms" << std::endl;

}

void PuzzleExperiment::run_once() {
	TimePoint start, end;
	QueryProfiler qp;

	LocalCacheManager lcm(CachingStrategy::by_name("always"),capacity, capacity, capacity, capacity, capacity);

	CacheManager::init(&lcm);
	for ( size_t i = 0; i < queries.size(); i++ ) {
		start = SysClock::now();
		execute_query(queries[i],qp);
		end = SysClock::now();
		accum[i] += duration(start,end);
	}
}



////////////////////////////////////////////////////////
//
// relevance experiment
//
////////////////////////////////////////////////////////

RelevanceExperiment::RelevanceExperiment(const QuerySpec& spec, uint32_t num_runs) :
	CacheExperimentSingleQuery("Relevance-Functions", spec, num_runs ), capacity(0) {
	if ( query_spec.epsg != EPSG_LATLON )
		throw ArgumentException("Only LatLon support for ReorgExperiment");
}

void RelevanceExperiment::global_setup() {
	auto q = generate_queries();
	TracingCacheManager tcm;
	CacheManager::init(&tcm);
	QueryProfiler qp;
	execute_query(q[0],qp);

	// Capacity to store all tiles
	capacity = (tcm.size * 16 * 5);
	for ( size_t i = 0; i < rels.size(); i++ )
		for ( size_t j = 0; j < ratios.size(); j++ )
			accums[i][j] = 0;
}

void RelevanceExperiment::setup() {
	queries = generate_queries();
}

void RelevanceExperiment::print_results() {
	for ( size_t i = 0; i < rels.size(); i++ ) {
		for ( size_t j = 0; j < ratios.size(); j++ )
			std::cout << rels[i] << "(" << ratios[j] << "): " << (accums[i][j]/num_runs) << "ms" <<std::endl;
	}
}

void RelevanceExperiment::run_once() {
	for ( size_t i = 0; i < rels.size(); i++ )
		for ( size_t j = 0; j < ratios.size(); j++ )
			exec(rels[i], capacity*ratios[j], accums[i][j]);
}

void RelevanceExperiment::exec(const std::string& relevance, size_t capacity, size_t &accum) {
	LocalTestSetup setup( 1, 1, 100, capacity, "geo", relevance, "always" );
	TimePoint start, end;
	for ( auto &q : queries ) {
		start = SysClock::now();
		execute_query(setup.get_client(),q);
		end = SysClock::now();
		accum += duration(start,end);
		//Pause between requests (for LRU);
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

std::vector<QTriple> RelevanceExperiment::generate_queries() {
	std::vector<QTriple> result;

	// [-112.5, -90  ], [ 22.5,  45  ] --> Nordamerika
	// [   0  ,  22.5], [ 45  ,  67,5] --> Europa
	// [   0  ,  22.5], [  0  ,  22,5] --> Afrika
	// [  67.5,  90  ], [ 22,5,  45  ] --> Asien
	// [ 135  , 157.5], [-45  , -22.5] --> Australien

	std::vector<SpatialReference> areas{
		SpatialReference(EPSG_LATLON, -112.5,  22.5, -90.0,  45.0 ), // Nordamerika
		SpatialReference(EPSG_LATLON,    0.0,  45.0,  22.5,  67.5 ), // Europa
		SpatialReference(EPSG_LATLON,    0.0,   0.0,  22.5,  22.5 ), // Afrika
		SpatialReference(EPSG_LATLON,   67.5,  22.5,  90.0,  45.0 ), // Asien
		SpatialReference(EPSG_LATLON,  135.0, -45.0, 157.5, -22.5 )  // Australien
	};

	std::default_random_engine generator;
	std::uniform_int_distribution<int> distribution(0,areas.size()-1);
	std::uniform_int_distribution<int> tdist(0,15);

	while ( result.size() < 160 ) {
		auto &area = areas.at(distribution(generator));
		int tile = tdist(generator);

		int x = tile % 4, y = tile / 4;

		double extend = (area.x2-area.x1) / 4;

		double x1 = area.x1 + x*extend,
			   x2 = area.x1 + (x+1)*extend,
			   y1 = area.y1 + y*extend,
			   y2 = area.y1 + (y+1)*extend;

		QueryRectangle qr(
				SpatialReference(EPSG_LATLON, x1,y1,x2,y2),
				query_spec.tref,
				QueryResolution::pixels(256,256)
		);
		result.push_back( QTriple(CacheType::RASTER,qr,query_spec.workflow) );
	}
	return result;
}

//
// batching experiment
//

QueryBatchingExperiment::QueryBatchingExperiment(const QuerySpec& spec, uint32_t num_runs )
	: CacheExperimentSingleQuery("Query-Batching", spec, num_runs ), capacity(0), queries_scheduled(0) {
}

void QueryBatchingExperiment::global_setup() {
	queries_scheduled = 0;
	capacity = 50*1024*1024;
	accum_batched.all_cpu = 0;
	accum_batched.all_gpu = 0;
	accum_batched.all_io = 0;
	accum_unbatched.all_cpu = 0;
	accum_unbatched.all_gpu = 0;
	accum_unbatched.all_io = 0;

}

void QueryBatchingExperiment::setup() {
	uint tiles = 4;
	queries.clear();
	QueryRectangle all = query_spec.random_rectangle_percent(1.0/8, 1024 );
	double dx = (all.x2-all.x1) / tiles;
	double dy = (all.y2-all.y1) / tiles;

	// Generate a tiles x tiles tiled query
	for ( uint x = 0; x < tiles; x++ ) {
		for ( uint y = 0; y < tiles; y++ ) {
			double x1 = all.x1 + x*dx;
			double y1 = all.y1 + y*dy;
			QueryRectangle qr(
					SpatialReference(all.epsg,x1,y1,x1+dx,y1+dy ),
					all,
					QueryResolution::pixels(all.xres/tiles,all.yres/tiles)
			);
			queries.push_back( QTriple(CacheType::RASTER,qr,query_spec.workflow) );
		}
	}

}

void QueryBatchingExperiment::print_results() {
	std::cout << "Queries scheduled: " << ((double) queries_scheduled / num_runs) << std::endl;
	std::cout << "Batched costs  : CPU: " << (accum_batched.all_cpu / num_runs) << ", GPU: " << (accum_batched.all_gpu / num_runs) << ", IO: " << (accum_batched.all_io / num_runs) << std::endl;
	std::cout << "Unbatched costs: CPU: " << (accum_unbatched.all_cpu / num_runs) << ", GPU: " << (accum_unbatched.all_gpu / num_runs) << ", IO: " << (accum_unbatched.all_io / num_runs) << std::endl;
}

void QueryBatchingExperiment::run_once() {
	auto s = CachingStrategy::by_name("never");
	LocalCacheManager lcm(std::move(s),capacity,capacity,capacity,capacity,capacity);
	CacheManager::init(&lcm);
	execute_query_steps(queries,accum_unbatched);
	exec(1,1);
}


void QueryBatchingExperiment::exec(int nodes, int threads) {

	LocalTestSetup setup( nodes, threads, 0, capacity, "geo", "costlru", "never" );

	std::deque<QTriple> queries;
	for ( auto &q : this->queries )
		queries.push_back(q);

	ParallelExecutor pe(queries,setup.get_client(), queries.size() );
	pe.execute();

	accum_batched += setup.get_manager().get_costs();
	queries_scheduled += setup.get_index().get_stats().queries_scheduled;
}

////////////////////////////////////////////////////////
//
// reorg experiment
//
////////////////////////////////////////////////////////

ReorgExperiment::ReorgExperiment(const std::vector<QuerySpec>& specs, uint32_t num_runs) :
  CacheExperimentMultiQuery("Reorganization", specs, num_runs), capacity(0) {
}

void ReorgExperiment::global_setup() {
	for ( int i = 0; i < 3; i++)
		accum[i].reset();

	auto qr = query_specs[0].random_rectangle_percent(1.0/32,256);

	QTriple t(CacheType::RASTER,qr,query_specs[0].workflow);

	TracingCacheManager tcm;
	CacheManager::init(&tcm);
	QueryProfiler qp;
	execute_query(t,qp);

	size_t s = tcm.size;
	capacity = s * 100 * 2;
}

void ReorgExperiment::setup() {
	std::default_random_engine generator;
	std::uniform_int_distribution<int> q_dist(0,query_specs.size()-1);

	step1.clear();
	step2.clear();

	while ( step1.size() < 100 ) {
		auto &spec = query_specs.at( q_dist(generator) );
		auto qr    = spec.random_rectangle_percent(1.0/32,256);
		auto steps = spec.get_query_steps(qr);
		step1.push_back(steps[1]);
		step2.push_back(steps[0]);
	}
}

void ReorgExperiment::print_results() {
	std::vector<std::string> strats{ "capacity", "graph", "geo" };
	for ( size_t i = 0; i < strats.size(); i++ ) {
		auto &avg = accum[i];
		double misses = (double) avg.misses / num_runs;
		double local  = (double) (avg.multi_local_hits+avg.multi_local_partials+avg.single_local_hits) / num_runs;
		double remote = (double) (avg.multi_remote_hits+avg.multi_remote_partials+avg.single_remote_hits) / num_runs;
		std::cout << "Average stats for strategy \"" << strats[i] << "\": " << std::endl;
		std::cout << "  Local hits : " << local  << std::endl;
		std::cout << "  Remote hits: " << remote << std::endl;
		std::cout << "  Misses     : " << misses << std::endl;
	}
}

void ReorgExperiment::run_once() {
	exec("capacity",accum[0]);
	exec("graph",accum[1]);
	exec("geo",accum[2]);
}

void ReorgExperiment::exec(const std::string& strategy, QueryStats& stats) {
	LocalTestSetup setup( 10, 1, 0, capacity, strategy, "costlru", "always" );

	std::default_random_engine generator;
	std::uniform_int_distribution<int> q_dist(0,1);

	size_t i = 0, j = 0;
	while ( i < step1.size() && j < step2.size() ) {
		if ( j < i && q_dist(generator) == 1 )
			execute_query(setup.get_client(), step2[j++] );
		else
			execute_query(setup.get_client(), step1[i++] );

		setup.get_index().force_reorg();
	}

	setup.get_index().force_stat_update();
	for ( auto &n: setup.get_nodes() )
		stats += n->get_cache_manager().get_query_stats();
}


////////////////////////////////////////////////////////
//
// caching strategy experiment
//
////////////////////////////////////////////////////////

StrategyExperiment::StrategyExperiment(const QuerySpec& spec, uint32_t num_runs)
 : CacheExperimentSingleQuery("Caching-Strategy", spec, num_runs ), capacity(0) {
}

void StrategyExperiment::global_setup() {
	capacity = 50*1024*1024; // 50M
	accums.clear();
}

void StrategyExperiment::setup() {
	auto bounds = SpatialReference::extent(query_spec.epsg);
	double extend = (bounds.x2 - bounds.x1) / 16;
	auto qr = query_spec.random_rectangle(extend,1024);
	queries = query_spec.get_query_steps(qr);
}

void StrategyExperiment::print_results() {
	for ( auto &p : accums ) {
		std::cout << p.first << ": " << (p.second.first/num_runs) << "ms, " << (p.second.second/num_runs) << " bytes" << std::endl;
	}
}

void StrategyExperiment::run_once() {
	exec(make_unique<CacheAll>(), get_accum("Always"));

	for ( int i = 1; i < 10; i++ )
		exec(make_unique<AuthmannStrategy>(i), get_accum(concat("Simple (",i,")")));

	for ( int fstacked = 2; fstacked < 10; fstacked++ )
		for ( int fimm = 1; fimm < fstacked; fimm++ ) {
			auto m = concat("TwoStep (", fstacked,",",fimm,")");
			exec(make_unique<TwoStepStrategy>(fstacked,fimm), get_accum(m));
		}
}

void StrategyExperiment::exec(std::unique_ptr<CachingStrategy> strategy, std::pair<uint64_t,uint64_t> &accum ) {
	QueryProfiler qp;
	LocalCacheManager lcm( std::move(strategy), capacity, capacity, capacity, capacity, capacity );
	CacheManager::init(&lcm);

	TimePoint start, end;
	start = SysClock::now();
	execute_query_steps(queries,qp);
	end = SysClock::now();

	auto cap = lcm.get_capacity();
	size_t bytes_used = cap.raster_cache_used + cap.point_cache_used +
						cap.line_cache_used + cap.polygon_cache_used +
						cap.plot_cache_used;

	accum.first  += duration(start,end);
	accum.second += bytes_used;
}

std::pair<uint64_t, uint64_t>& StrategyExperiment::get_accum(
		const std::string& key) {
	auto it = accums.find(key);
	if ( it == accums.end() ) {
		std::pair<uint64_t,uint64_t> accum(0,0);
		return accums.emplace(key, accum).first->second;
	}
	else
		return it->second;
}








////////////////////////////////////////////////////////
//
// Reorg experiment
//
////////////////////////////////////////////////////////

ReorgExperimentOld::ReorgExperimentOld(const QuerySpec& spec, uint32_t num_runs) :
	CacheExperimentSingleQuery("Reorganization", spec, num_runs ), capacity(0) {
}

void ReorgExperimentOld::global_setup() {
	capacity = 50*1024*1024; // 50M
	for ( int i = 0; i < 3; i++)
		accum[i].reset();
}

void ReorgExperimentOld::setup() {
	step1.clear();
	step2.clear();


	auto bounds = SpatialReference::extent(query_spec.epsg);
	double extend = (bounds.x2 - bounds.x1) / 150;
	uint32_t res = 256;

	// Create disjunct rectangles
	std::vector<QueryRectangle> rects;
	while( rects.size() < 100 ) {
		auto rect = query_spec.random_rectangle(extend,res);
		bool add = true;
		for ( auto &r : rects  ) {
			 add &= ( rect.x2 < r.x1 || rect.x1 > r.x2 ) && ( rect.y2 < r.y1 || rect.y1 > r.y2 );
		}
		if ( add )
			rects.push_back( rect );
	}

	for ( auto &q : rects ) {
		auto v = query_spec.get_query_steps(q);
		step1.push_back( v[1] );
		step2.push_back( v[0] );
	}
}

void ReorgExperimentOld::print_results() {
	std::vector<std::string> strats{ "capacity", "graph", "geo" };
	for ( size_t i = 0; i < strats.size(); i++ ) {
		auto &avg = accum[i];
		double misses = (double) avg.misses / num_runs;
		double local  = (double) (avg.multi_local_hits+avg.multi_local_partials+avg.single_local_hits) / num_runs;
		double remote = (double) (avg.multi_remote_hits+avg.multi_remote_partials+avg.single_remote_hits) / num_runs;
		std::cout << "Average stats for strategy \"" << strats[i] << "\": " << std::endl;
		std::cout << "  Local hits : " << local  << std::endl;
		std::cout << "  Remote hits: " << remote << std::endl;
		std::cout << "  Misses     : " << misses << std::endl;
	}
}

void ReorgExperimentOld::run_once() {
	exec("capacity",accum[0]);
	exec("graph",accum[1]);
	exec("geo",accum[2]);

}

void ReorgExperimentOld::exec(const std::string& strategy, QueryStats &stats) {
	LocalTestSetup setup( 10, 1, 0, capacity, strategy, "costlru", "always" );
	for ( auto &q : step1 ) {
		execute_query(setup.get_client(),q);
	}

	// Force reorg
	setup.get_index().force_reorg();
	setup.get_index().reset_stats();
	for ( auto &n: setup.get_nodes() )
		n->get_cache_manager().reset_query_stats();

	// Process next step
	for ( auto &q : step2 )
		execute_query(setup.get_client(),q);

	setup.get_index().force_stat_update();

	for ( auto &n: setup.get_nodes() )
		stats += n->get_cache_manager().get_query_stats();
}

//RelevanceExperimentOld::RelevanceExperimentOld(const QuerySpec& spec, uint32_t num_runs)
//	: CacheExperiment("Relevance", spec, num_runs ), capacity(0) {
//}
//
//void RelevanceExperimentOld::global_setup() {
//	for ( size_t i = 0; i < rels.size(); i++ ) {
//		accums[i] = 0;
//	}
//}
//
//void RelevanceExperimentOld::setup() {
//	queries.clear();
//	int num_trips = 3;
//	std::vector<QueryRectangle> trip;
//	SpatialReference ex = SpatialReference::extent(query_spec.epsg);
//
//	double extend = (ex.y2-ex.y1) / 10;
//	double x1 = ex.x1, y1 = ex.y1;
//
//	// Move up
//	for ( ; y1 < ex.y2; y1 += extend )
//		trip.push_back( query_spec.rectangle(x1,y1,extend,1024) );
//
//	// Move right
//	y1 -= extend;
//	x1 += extend;
//	for ( ; x1 < ex.x2; x1+=extend )
//		trip.push_back( query_spec.rectangle(x1,y1,extend,1024) );
//
//	// Move down
//	x1 -= extend;
//	y1 -= extend;
//	for ( ; y1 >= ex.y1; y1 -= extend )
//		trip.push_back( query_spec.rectangle(x1,y1,extend,1024) );
//
//	// Move left
//	y1 += extend;
//	x1 -= extend;
//	for ( ; x1 > ex.x1; x1-=extend )
//		trip.push_back( query_spec.rectangle(x1,y1,extend,1024) );
//
////	for ( auto &q : trip ) {
////		printf("[%f,%f]x[%f,%f]\n", q.x1,q.x2,q.y1,q.y2);
////	}
//
////	fprintf(stderr,"Calculating required cache-size for 1 trip\n");
//	TracingCacheManager ncm;
//	CacheManager::init(&ncm);
//	auto op = GenericOperator::fromJSON(query_spec.workflow);
//	for ( auto &q : trip ) {
//		QueryProfiler qp;
//		op->getCachedRaster(q,qp);
//	}
//	capacity = ncm.size / 2;
////	fprintf(stderr,"Finished calculating required cache-size for 1 trip: %ld bytes\n", trip_cache_size);
//
//
//	queries.reserve( num_trips * trip.size() );
//
//	for ( int i = 0; i < num_trips; i++ ) {
//		for ( auto &q : trip ) {
//			queries.push_back( QTriple(CacheType::RASTER,q,query_spec.workflow) );
//		}
//	}
//}
//
//void RelevanceExperimentOld::print_results() {
//	for ( size_t i = 0; i < rels.size(); i++ ) {
//		std::cout << rels[i] << ": " << (accums[i]/num_runs) << "ms" <<std::endl;
//	}
//
//}
//
//void RelevanceExperimentOld::run_once() {
//	for ( size_t i = 0; i < rels.size(); i++ ) {
//		exec(rels[i], accums[i]);
//	}
//}
//
//void RelevanceExperimentOld::exec(const std::string& relevance, size_t &accum) {
//	LocalTestSetup setup( 1, 1, 2000, capacity, "geo", relevance, "always" );
//	TimePoint start, end;
//	for ( auto &q : queries ) {
//		start = SysClock::now();
//		execute_query(setup.get_client(),q);
//		end = SysClock::now();
//		accum += duration(start,end);
//		//Pause between requests (for LRU);
//		std::this_thread::sleep_for(std::chrono::milliseconds(200));
//	}
//}
