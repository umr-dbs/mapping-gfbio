/*
 * cluster_experiment.cpp
 *
 *  Created on: 19.01.2016
 *      Author: mika
 */

#include "cache/experiments/exp_workflows.h"
#include "raster/opencl.h"
#include "util/configuration.h"

class Spec {
public:
	Spec(const QuerySpec &spec,uint32_t tiles,uint32_t res) :
		spec(spec),tiles(tiles),res(res) {}
	const QuerySpec &spec;
	uint32_t tiles;
	uint32_t res;
};

void execute( Spec &s ) {

	std::string host = Configuration::get("indexserver.host");
	int port = atoi( Configuration::get("indexserver.port").c_str() );
	int num_threads = atoi( Configuration::get("experiment.threads").c_str() );
	size_t num_queries = atoi( Configuration::get("experiment.queries").c_str() );


	std::default_random_engine eng(std::chrono::system_clock::now().time_since_epoch().count());
	std::uniform_int_distribution<uint16_t> dist(0, s.tiles*s.tiles-1);

	double extend = std::min(
		(s.spec.bounds.x2 - s.spec.bounds.x1) / 32,
		(s.spec.bounds.y2 - s.spec.bounds.y1) / 32
	);

	std::deque<QTriple> queries;
	for ( size_t i = 0; i < num_queries; i++ ) {
		uint16_t tile = dist(eng);
		uint16_t y = tile / 32;
		uint16_t x = tile % 32;

		double x1 = s.spec.bounds.x1 + x * extend;
		double y1 = s.spec.bounds.y1 + y * extend;

		QueryRectangle qr = s.spec.rectangle(x1,y1,extend,256);
		queries.push_back( QTriple(CacheType::RASTER,qr,s.spec.workflow) );
	}

	ClientCacheManager ccm(host,port);
	ParallelExecutor pe(queries,ccm,num_threads);

	CacheExperiment::TimePoint start, end;
	start = CacheExperiment::SysClock::now();
	pe.execute();
	end = CacheExperiment::SysClock::now();
	printf("Execution of %ld queries took: %ldms\n", num_queries, CacheExperiment::duration(start,end) );
}

int main(int argc, const char* argv[]) {
	Configuration::loadFromDefaultPaths();
	Configuration::load("cluster_experiment.conf");
	Log::setLevel( Configuration::get("log.level") );

	std::vector<Spec> specs{
		Spec( cache_exp::avg_temp, 32, 256 ),
		Spec( cache_exp::srtm_ex, 32, 256 ),
		Spec( cache_exp::srtm_proj, 32, 256 ),
		Spec( cache_exp::cloud_detection, 14, 256 )
	};

	if ( argc < 2 ) {
		printf("Usage: %s [1-%lu]\n", argv[0], specs.size());
		exit(1);
	}

	int num = atoi(argv[1]);
	if ( num < 1 || num > specs.size() ) {
		printf("Usage: %s [1-%lu]\n", argv[0], specs.size());
		exit(1);
	}

	execute( specs[num-1] );

//	int exp = 0;
//
//	do {
//		std::cout << "Select the query-type to use (-1 for exit):" << std::endl;
//		std::cout << " [0] All" << std::endl;
//		for ( size_t i = 0; i < specs.size(); i++ ) {
//			std::cout << " [" << (i+1) << "] " << specs[i].name << std::endl;
//		}
//		std:: cout << "Your choice: ";
//		std::cin >> exp;
//
//		if ( exp > 0 && exp <= (ssize_t) specs.size() )
//			execute( specs[exp-1] );
//		else if ( exp == 0 )
//			for ( auto &s : specs ) {
//				execute(s);
//			}
//	} while ( exp >= 0 );
//	std::cout << "Bye" << std::endl;
	return 0;
}
