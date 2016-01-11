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
	LocalTestSetup setup( 10, 1, 10 * 1024 * 1024, strategy, "always" );
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

#endif
