/*
 * experiments.cpp
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */



#include <gtest/gtest.h>
#include <chrono>
#include "util/configuration.h"
#include "test/unittests/cache/util.h"

typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
typedef std::chrono::system_clock SysClock;

size_t duration( const TimePoint &start, const TimePoint &end ) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

#if 0

////////////////////////////////////////////////////////
//
// TEST CACHE
//
////////////////////////////////////////////////////////

TEST(CacheExperiments,LocalCacheExperiment) {
	Configuration::loadFromDefaultPaths();
	auto strategy = CachingStrategy::by_name("always");
	size_t capacity = 10*1024*1024;
	NopCacheManager nopmgr;
	LocalCacheManager lcm(std::move(strategy), capacity, capacity, capacity, capacity, capacity );


	time_t timestamp = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	std::string workflow = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	QueryRectangle qr = random_rect(EPSG_WEBMERCATOR, 10018754.17, timestamp, 1024 );

	// Get queries
	auto queries = get_query_steps( workflow, qr, CacheType::RASTER );
	// Uncached
	CacheManager::init(&nopmgr);

	TimePoint start, end;
	auto iter = queries.rbegin();
	while ( iter != queries.rend() ) {
		start = SysClock::now();
		execute(*iter);
		end = SysClock::now();
		fprintf(stderr, "Uncached time: %ld\n", duration(start,end) );
		iter++;
	}

	// Cached
	CacheManager::init(&lcm);
	iter = queries.rbegin();
	while ( iter != queries.rend() ) {
		start = SysClock::now();
		execute(*iter);
		end = SysClock::now();
		fprintf(stderr, "Cached time: %ld\n", duration(start,end) );
		iter++;
	}
}

////////////////////////////////////////////////////////
//
// TEST PUZZLE
//
////////////////////////////////////////////////////////


TEST(CacheExperiments,PuzzlExperiment) {
	Configuration::loadFromDefaultPaths();
	auto strategy = CachingStrategy::by_name("always");
	size_t capacity = 10*1024*1024;
	NopCacheManager nopmgr;
	LocalCacheManager lcm(std::move(strategy), capacity, capacity, capacity, capacity, capacity );


	time_t timestamp = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	std::string workflow = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";

	double x1 = -20037508.34, x2 = -10018754.17;
	double d = (x2-x1) / 4;



	std::vector<QTriple> query_steps;
	for ( int i = 0; i < 4; i++ ) {
		QueryRectangle qr = QueryRectangle(
				SpatialReference( EPSG_WEBMERCATOR, x1 + i*d, -10018754.17, x2 + i*d, 0 ),
				TemporalReference( TIMETYPE_UNIX, timestamp, timestamp ),
				QueryResolution::pixels(1024,1024)
		);
		query_steps.push_back( QTriple(CacheType::RASTER, qr, workflow ) );
	}

	// Uncached
	CacheManager::init(&nopmgr);
	execute( query_steps[0] );

	for ( size_t i = 1; i < query_steps.size(); i++ ) {
		TimePoint start = SysClock::now();
		execute( query_steps[i] );
		TimePoint end = SysClock::now();
		fprintf(stderr, "Uncached time (%ld/4 overlap): %ld\n", (4-i),duration(start,end) );
	}

	CacheManager::init(&lcm);
	execute( query_steps[0] );

	lcm.set_strategy( CachingStrategy::by_name("never") );

	for ( size_t i = 1; i < query_steps.size(); i++ ) {
		TimePoint start = SysClock::now();
		execute( query_steps[i] );
		TimePoint end = SysClock::now();
		fprintf(stderr, "Cached time (%ld/4 overlap): %ld\n", (4-i),duration(start,end) );
	}
}



////////////////////////////////////////////////////////
//
// TEST REORG
//
////////////////////////////////////////////////////////

void run_reorg_test( const std::string &strategy, std::vector<QTriple> &step1, std::vector<QTriple> &step2 );

TEST(CacheExperiments,ReorgExperiment) {
	Configuration::loadFromDefaultPaths();
	std::string workflow = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	epsg_t epsg = EPSG_WEBMERCATOR;
	auto ext = SpatialReference::extent(EPSG_WEBMERCATOR);
	double extend = (ext.x2 - ext.x1) / 150;
	double time = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	uint32_t res = 256;

	std::vector<QueryRectangle> rects;
	// Create disjunct rectangles
	while( rects.size() < 100 ) {
		auto rect = random_rect(epsg,extend,time,res);
		bool add = true;
		for ( auto &r : rects  ) {
			 add &= ( rect.x2 < r.x1 || rect.x1 > r.x2 ) && ( rect.y2 < r.y1 || rect.y1 > r.y2 );
		}
		if ( add )
			rects.push_back( rect );
	}

	std::vector<QTriple> step1;
	std::vector<QTriple> step2;

	for ( auto &q : rects ) {
		auto v = get_query_steps(workflow,q,CacheType::RASTER);
		step1.push_back( v[1] );
		step2.push_back( v[0] );
	}

	run_reorg_test("capacity", step1, step2);
	run_reorg_test("graph", step1, step2);
	run_reorg_test("geo", step1, step2);
}

void run_reorg_test( const std::string &strategy, std::vector<QTriple> &step1, std::vector<QTriple> &step2 ) {
	LocalTestSetup setup( 10, 1, 0, 10 * 1024 * 1024, strategy, "lru", "always" );
	for ( auto &q : step1 ) {
		auto op = GenericOperator::fromJSON(q.semantic_id);
		setup.get_client().get_raster_cache().query(*op,q.query);
	}

	// Force reorg
	setup.get_index().force_reorg();
	setup.get_index().reset_stats();
	for ( auto &n: setup.get_nodes() )
		n->get_cache_manager().reset_query_stats();

	// Process next step
	for ( auto &q : step2 ) {
		auto op = GenericOperator::fromJSON(q.semantic_id);
		setup.get_client().get_raster_cache().query(*op,q.query);
	}
	setup.get_index().force_stat_update();

	QueryStats cumulated;
	for ( auto &n: setup.get_nodes() )
		cumulated += n->get_cache_manager().get_query_stats();

	Log::error("Finished reorg-experiment: %s\n%s",strategy.c_str(), cumulated.to_string().c_str() );
}


////////////////////////////////////////////////////////
//
// TEST CACHING STRATEGY
//
////////////////////////////////////////////////////////

std::pair<size_t,size_t> run_strategy_exp( std::unique_ptr<CachingStrategy> strategy, std::vector<QTriple> &queries );

TEST(CacheExperiments,CachingStrategy) {
	Configuration::loadFromDefaultPaths();

	time_t timestamp = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	std::string workflow = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	QueryRectangle qr = random_rect(EPSG_WEBMERCATOR, 10018754.17, timestamp, 1024 );

	// Get queries
	auto queries = get_query_steps( workflow, qr, CacheType::RASTER );

	{
		auto res = run_strategy_exp(make_unique<CacheAll>(), queries);
		fprintf(stderr, "Result for CacheAll: %ld/%ld\n", res.first, res.second );
	}

	for ( double f = 1; f < 10; f++ ) {
		auto res = run_strategy_exp(make_unique<AuthmannStrategy>(f), queries);
		fprintf(stderr, "Result for SimpleStrategy(%f): %ld/%ld\n", f, res.first, res.second );
	}

	for ( double fstacked = 2; fstacked < 10; fstacked++ ) {
		for ( double fimm = 1; fimm < fstacked; fimm++ ) {
			for ( uint16_t stackdepth = 1; stackdepth < 6; stackdepth++ ) {
				auto res = run_strategy_exp(make_unique<TwoStepStrategy>(fstacked,fimm,stackdepth), queries);
				fprintf(stderr, "Result for TwoStepStrategy(%f,%f,%d): %ld/%ld\n", fstacked,fimm,stackdepth, res.first, res.second );
			}
		}
	}
}

std::pair<size_t,size_t> run_strategy_exp( std::unique_ptr<CachingStrategy> strategy,std::vector<QTriple> &queries ) {
	size_t capacity = 10 * 1024 * 1024; // 10 MiB
	LocalCacheManager lcm(std::move(strategy), capacity, capacity, capacity, capacity, capacity );

	CacheManager::init(&lcm);

	TimePoint start, end;
	auto iter = queries.rbegin();
	start = SysClock::now();
	while ( iter != queries.rend() ) {
		execute(*iter);
		iter++;
	}
	end = SysClock::now();

	auto cap = lcm.get_capacity();

	size_t bytes_used = cap.raster_cache_used + cap.point_cache_used +
						cap.line_cache_used + cap.polygon_cache_used +
						cap.plot_cache_used;

	return std::pair<size_t,size_t>( duration(start,end), bytes_used );
}



////////////////////////////////////////////////////////
//
// TEST RELEVANCE FUNCTIONS
//
////////////////////////////////////////////////////////


size_t run_relevance_exp( const std::string &relevance, size_t capacity, std::vector<QTriple> &queries );

TEST(CacheExperiments,RelevanceFunction) {
	Configuration::loadFromDefaultPaths();
	time_t timestamp = parseIso8601DateTime("2010-06-06T18:00:00.000Z");
	std::string workflow = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";

	int num_trips = 3;

	std::vector<QueryRectangle> trip;

	epsg_t epsg = EPSG_WEBMERCATOR;
	SpatialReference ex = SpatialReference::extent(epsg);

	double extend = (ex.y2-ex.y1) / 10;
	double x1 = ex.x1, y1 = ex.y1;

	// Move up
	for ( ; y1 < ex.y2; y1 += extend ) {
		trip.push_back( rect(epsg, x1,y1,extend,timestamp,1024) );
	}
	y1 -= extend;
	x1 += extend;

	// Move right
	for ( ; x1 < ex.x2; x1+=extend ) {
		trip.push_back( rect(epsg, x1,y1,extend,timestamp,1024) );
	}
	x1 -= extend;
	y1 -= extend;

	// Move down
	for ( ; y1 >= ex.y1; y1 -= extend ) {
		trip.push_back( rect(epsg, x1,y1,extend,timestamp,1024) );
	}
	// Move left
	y1 += extend;
	x1 -= extend;

	for ( ; x1 > ex.x1; x1-=extend ) {
		trip.push_back( rect(epsg, x1,y1,extend,timestamp,1024) );
	}

//	for ( auto &q : trip ) {
//		printf("[%f,%f]x[%f,%f]\n", q.x1,q.x2,q.y1,q.y2);
//	}

	fprintf(stderr,"Calculating required cache-size for 1 trip\n");
	TracingCacheManager ncm;
	CacheManager::init(&ncm);
	auto op = GenericOperator::fromJSON(workflow);
	for ( auto &q : trip ) {
		QueryProfiler qp;
		op->getCachedRaster(q,qp);
	}
	size_t trip_cache_size = ncm.size;
	fprintf(stderr,"Finished calculating required cache-size for 1 trip: %ld bytes\n", trip_cache_size);


	std::vector<QTriple> steps;
	steps.reserve( num_trips * trip.size() );

	for ( int i = 0; i < num_trips; i++ ) {
		for ( auto &q : trip ) {
			steps.push_back( QTriple(CacheType::RASTER,q,workflow) );
		}
	}

	size_t dur = run_relevance_exp( "lru", trip_cache_size/2, steps );

	fprintf(stderr, "Execution time LRU: %ld\n", dur);

	dur = run_relevance_exp( "costlru", trip_cache_size/2, steps );

	fprintf(stderr, "Execution time LRU-COST: %ld\n", dur);

}

size_t run_relevance_exp( const std::string &relevance, size_t capacity, std::vector<QTriple> &queries ) {
	LocalTestSetup setup( 1, 1, 2, capacity, "geo", relevance, "always" );

	size_t res = 0;
	TimePoint start, end;

	int c = 1;
	for ( auto &q : queries ) {
		auto op = GenericOperator::fromJSON(q.semantic_id);
		start = SysClock::now();
		setup.get_client().get_raster_cache().query(*op,q.query);
		end = SysClock::now();
//		fprintf(stderr,"Executed %d/%ld\n", c++, queries.size());
		res += duration(start,end);
		//Pause between requests (for LRU);
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
	return res;
}

#endif







