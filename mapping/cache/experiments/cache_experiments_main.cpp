/*
 * cache_experiments_main.cpp
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#include "cache/experiments/cache_experiments.h"
#include "cache/experiments/exp_workflows.h"
#include "rasterdb/rasterdb.h"
#include "util/configuration.h"
#include <vector>
#include <memory>
#include <iostream>
#include <stdio.h>
#include <gdal_priv.h>
#include <random>


//void doit() {
//	auto &query_spec = cache_exp::avg_temp;
//	std::vector<QTriple> result;
//
//	// [-112.5, -90  ], [ 22.5,  45  ] --> Nordamerika
//	// [   0  ,  22.5], [ 45  ,  67,5] --> Europa
//	// [   0  ,  22.5], [  0  ,  22,5] --> Afrika
//	// [  67.5,  90  ], [ 22,5,  45  ] --> Asien
//	// [ 135  , 157.5], [-45  , -22.5] --> Australien
//
//	std::vector<SpatialReference> areas{
//		SpatialReference(EPSG_LATLON, -112.5,  22.5, -90.0,  45.0 ), // Nordamerika
//		SpatialReference(EPSG_LATLON,    0.0,  45.0,  22.5,  67.5 ), // Europa
//		SpatialReference(EPSG_LATLON,    0.0,   0.0,  22.5,  22.5 ), // Afrika
//		SpatialReference(EPSG_LATLON,   67.5,  22.5,  90.0,  45.0 ), // Asien
//		SpatialReference(EPSG_LATLON,  135.0, -45.0, 157.5, -22.5 )  // Australien
//	};
//
//	std::default_random_engine generator;
//	std::uniform_int_distribution<int> distribution(0,areas.size()-1);
//	std::uniform_int_distribution<int> tdist(0,15);
//
//	while ( result.size() < 160 * 100 ) {
//		auto &area = areas.at(distribution(generator));
//		int tile = tdist(generator);
//
//		int x = tile % 4, y = tile / 4;
//
//		double extend = (area.x2-area.x1) / 4;
//
//		double x1 = area.x1 + x*extend,
//			   x2 = area.x1 + (x+1)*extend,
//			   y1 = area.y1 + y*extend,
//			   y2 = area.y1 + (y+1)*extend;
//
//		QueryRectangle qr(
//				SpatialReference(EPSG_LATLON, x1,y1,x2,y2),
//				query_spec.tref,
//				QueryResolution::pixels(256,256)
//		);
//		result.push_back( QTriple(CacheType::RASTER,qr,query_spec.workflow) );
//	}
//
//	//LocalCacheManager lcm(CachingStrategy::by_name("never"),100,100,100,100,100);
////	NopCacheManager ncm;
////	CacheManager::init(&ncm);
//
//	LocalTestSetup setup(1,1,100,50000000,"geo","lru","always");
//
//	printf("Starting execution in 5 secs\n");
//	std::this_thread::sleep_for(std::chrono::milliseconds(5000));
//	printf("Starting execution\n");
//	auto op = GenericOperator::fromJSON(query_spec.workflow);
//	for ( auto &q : result ) {
////		QueryProfiler qp;
////		auto res = op->getCachedRaster(q.query, qp);
//		setup.get_client().get_raster_cache().query(*op,q.query);
////		std::this_thread::sleep_for(std::chrono::milliseconds(10));
//	}
//	printf("Finished execution\n");
//	std::this_thread::sleep_for(std::chrono::milliseconds(5000));
//	RasterOpenCL::free();
//	printf("Freed opencl context\n");
//}

void GDALErrorHandler(CPLErr eErrClass, int err_no, const char *msg) {
	(void) eErrClass;
	(void) err_no;
	(void) msg;
}


int main(void) {
	// Swallow stderr;
//	freopen("/dev/null","w",stderr);
	Configuration::load("local_experiments.conf");
	Log::setLevel( Configuration::get("log.level") );
	// Open all dbs

	CPLSetErrorHandler(GDALErrorHandler);

	std::vector<std::shared_ptr<RasterDB>> dbs{
		RasterDB::open("srtm", RasterDB::READ_ONLY),
		RasterDB::open("glc2000_global", RasterDB::READ_ONLY),
		RasterDB::open("worldclim", RasterDB::READ_ONLY),
		RasterDB::open("msg9_geos", RasterDB::READ_ONLY)
	};

//	make_unique<RelevanceExperiment>(cache_exp::avg_temp, 1)->run();

	size_t num_runs = 1;
	std::cout << "Enter the number of runs per experiment: ";
	std::cin >> num_runs;

	std::vector<std::unique_ptr<CacheExperiment>> experiments;
	experiments.push_back( make_unique<LocalCacheExperiment>(cache_exp::avg_temp, num_runs, 1.0/16, 1024) );
	experiments.push_back( make_unique<LocalCacheExperiment>(cache_exp::cloud_detection, num_runs, 1.0/4, 512) );
	experiments.push_back( make_unique<PuzzleExperiment>(cache_exp::avg_temp, num_runs, 1.0/16, 1024) );
	experiments.push_back( make_unique<PuzzleExperiment>(cache_exp::cloud_detection, num_runs, 1.0/4, 512) );
//	experiments.push_back( make_unique<QueryBatchingExperiment>(cache_exp::avg_temp, num_runs) );
//	experiments.push_back( make_unique<QueryBatchingExperiment>(cache_exp::cloud_detection, num_runs) );
//	experiments.push_back( make_unique<RelevanceExperiment>(cache_exp::avg_temp, num_runs) );

	// OK TO HERE


//	experiments.push_back( make_unique<ReorgExperiment>(qs, 2) );
//	experiments.push_back( make_unique<StrategyExperiment>(qs, 1) );

	int exp = 0;

	do {
		std::cout << "Choose the experiment to run (-1 for exit):" << std::endl;
		std::cout << " [0] All" << std::endl;
		for ( size_t i = 0; i < experiments.size(); i++ ) {
			std::cout << " [" << (i+1) << "] " << experiments[i]->name << std::endl;
		}
		std:: cout << "Your choice: ";
		std::cin >> exp;

		if ( exp > 0 && exp <= (ssize_t) experiments.size() )
			experiments[exp-1]->run();
		else if ( exp == 0 )
			for ( auto & e : experiments )
				e->run();
	} while ( exp >= 0 );
	std::cout << "Bye" << std::endl;

	return 0;
}
