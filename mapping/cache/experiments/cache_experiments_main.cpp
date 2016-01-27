/*
 * cache_experiments_main.cpp
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#include "cache/experiments/cache_experiments.h"
#include "cache/experiments/exp_workflows.h"
#include "rasterdb/rasterdb.h"
#include "raster/opencl.h"
#include "util/configuration.h"
#include <vector>
#include <memory>
#include <iostream>
#include <stdio.h>
#include <gdal_priv.h>



void GDALErrorHandler(CPLErr eErrClass, int err_no, const char *msg) {
	(void) eErrClass;
	(void) err_no;
	(void) msg;
}

int main(int argc, const char* argv[]) {
	CachingStrategy::init();
	Configuration::loadFromDefaultPaths();
	Configuration::load("local_experiments.conf");
	Log::setLevel( Configuration::get("log.level") );

	// Init opencl
#ifndef MAPPING_NO_OPENCL
	RasterOpenCL::init();
#endif

	// Disable GDAL Error Messages
	CPLSetErrorHandler(GDALErrorHandler);

	// Open all dbs
	std::vector<std::shared_ptr<RasterDB>> dbs{
		RasterDB::open("srtm", RasterDB::READ_ONLY),
//		RasterDB::open("glc2000_global", RasterDB::READ_ONLY),
		RasterDB::open("worldclim", RasterDB::READ_ONLY),
		RasterDB::open("msg9_geos", RasterDB::READ_ONLY)
	};

	// Queries for reorg
	std::vector<QuerySpec> qs1{
		cache_exp::shifted_temp("1995-01-15 12:00:00"),
		cache_exp::shifted_temp("1995-02-15 12:00:00"),
		cache_exp::shifted_temp("1995-03-15 12:00:00"),
		cache_exp::shifted_temp("1995-04-15 12:00:00"),
		cache_exp::shifted_temp("1995-05-15 12:00:00"),
		cache_exp::shifted_temp("1995-06-15 12:00:00"),
		cache_exp::shifted_temp("1995-07-15 12:00:00"),
		cache_exp::shifted_temp("1995-08-15 12:00:00"),
		cache_exp::shifted_temp("1995-09-15 12:00:00"),
		cache_exp::shifted_temp("1995-10-15 12:00:00"),
		cache_exp::shifted_temp("1995-11-15 12:00:00"),
		cache_exp::shifted_temp("1995-12-15 12:00:00"),
	};

	std::vector<QuerySpec> qs2{
		cache_exp::projected_shifted_temp("1995-01-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-02-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-03-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-04-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-05-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-06-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-07-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-08-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-09-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-10-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-11-15 12:00:00"),
		cache_exp::projected_shifted_temp("1995-12-15 12:00:00")
	};


	if ( argc < 3 ) {
		printf("Usage: %s #num_runs [1-12]\n", argv[0]);
		exit(1);
	}

	int num_runs = atoi(argv[1]);
	int exp = atoi(argv[2]);

	if ( num_runs < 1 ) {
		printf("Usage: %s #num_runs [1-12]\n", argv[0]);
		exit(1);
	}


//	size_t num_runs = 1;
//	std::cout << "Enter the number of runs per experiment: ";
//	std::cin >> num_runs;

	std::vector<std::unique_ptr<CacheExperiment>> experiments;
	experiments.push_back( make_unique<LocalCacheExperiment>(cache_exp::avg_temp, num_runs, 1.0/8, 1024) );
	experiments.push_back( make_unique<LocalCacheExperiment>(cache_exp::cloud_detection, num_runs, 1.0/3, 1024) );
	experiments.push_back( make_unique<PuzzleExperiment>(cache_exp::avg_temp, num_runs, 1.0/8, 1024) );
	experiments.push_back( make_unique<PuzzleExperiment>(cache_exp::cloud_detection, num_runs, 1.0/3, 1024) );
	experiments.push_back( make_unique<StrategyExperiment>(cache_exp::avg_temp, num_runs, 1.0/8, 1024) );
	experiments.push_back( make_unique<StrategyExperiment>(cache_exp::cloud_detection, num_runs, 1.0/4, 512) );

	experiments.push_back( make_unique<QueryBatchingExperiment>(cache_exp::avg_temp, num_runs) );
	experiments.push_back( make_unique<QueryBatchingExperiment>(cache_exp::cloud_detection, num_runs) );
	experiments.push_back( make_unique<ReorgExperiment>(qs1, num_runs) );
	experiments.push_back( make_unique<ReorgExperiment>(qs2, num_runs) );
	experiments.push_back( make_unique<RelevanceExperiment>(cache_exp::avg_temp, num_runs) );
	experiments.push_back( make_unique<RelevanceExperiment>(cache_exp::srtm_ex, num_runs) );

	if ( exp < 1 || exp > experiments.size() ) {
		printf("Usage: %s #num_runs [1-%lu]\n", argv[0], experiments.size());
		exit(1);
	}

	experiments[exp-1]->run();

//
//	int exp = 0;
//
//	do {
//		std::cout << "Choose the experiment to run (-1 for exit):" << std::endl;
//		std::cout << " [0] All" << std::endl;
//		for ( size_t i = 0; i < experiments.size(); i++ ) {
//			std::cout << " [" << (i+1) << "] " << experiments[i]->name << std::endl;
//		}
//		std:: cout << "Your choice: ";
//		std::cin >> exp;
//
//		if ( exp > 0 && exp <= (ssize_t) experiments.size() )
//			experiments[exp-1]->run();
//		else if ( exp == 0 )
//			for ( auto & e : experiments )
//				e->run();
//	} while ( exp >= 0 );
//	std::cout << "Bye" << std::endl;

	return 0;
}
