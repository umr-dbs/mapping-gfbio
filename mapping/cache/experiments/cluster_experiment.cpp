/*
 * cluster_experiment.cpp
 *
 *  Created on: 19.01.2016
 *      Author: mika
 */

#include "cache/experiments/exp_workflows.h"
#include "raster/opencl.h"
#include "util/configuration.h"

void execute( QuerySpec &spec ) {

	std::string host = Configuration::get("indexserver.host");
	int port = atoi( Configuration::get("indexserver.port").c_str() );
	int num_threads = atoi( Configuration::get("experiment.threads").c_str() );
	size_t num_queries = atoi( Configuration::get("experiment.queries").c_str() );

	std::deque<QTriple> queries;
	for ( size_t i = 0; i < num_queries; i++ ) {
		QueryRectangle qr = spec.random_rectangle_percent(1.0/64,256);
		queries.push_back( QTriple(CacheType::RASTER,qr,spec.workflow) );
	}

	ClientCacheManager ccm(host,port);
	ParallelExecutor pe(queries,ccm,num_threads);

	CacheExperiment::TimePoint start, end;
	start = CacheExperiment::SysClock::now();
	pe.execute();
	end = CacheExperiment::SysClock::now();
	printf("Execution of %ld queries took: %ldms\n", num_queries, CacheExperiment::duration(start,end) );
}

int main(void) {
	Configuration::load("cluster_experiment.conf");
	Log::setLevel( Configuration::get("log.level") );

	std::vector<QuerySpec> specs{
		cache_exp::avg_temp,
		cache_exp::srtm_ex,
		cache_exp::srtm_proj
	};

	RasterOpenCL::init();

	int exp = 0;

	do {
		std::cout << "Select the query-type to use (-1 for exit):" << std::endl;
		std::cout << " [0] All" << std::endl;
		for ( size_t i = 0; i < specs.size(); i++ ) {
			std::cout << " [" << (i+1) << "] " << specs[i].name << std::endl;
		}
		std:: cout << "Your choice: ";
		std::cin >> exp;

		if ( exp > 0 && exp <= (ssize_t) specs.size() )
			execute( specs[exp-1] );
		else if ( exp == 0 )
			for ( auto &s : specs ) {
				execute(s);
			}
	} while ( exp >= 0 );
	std::cout << "Bye" << std::endl;
}
