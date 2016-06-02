#include "cache/experiments/exp_util.h"
#include "cache/experiments/exp_workflows.h"
#include "util/configuration.h"
#include "util/log.h"
#include "raster/opencl.h"
#include <signal.h>

int num_nodes = 10;
int workers_per_node = 1;
int index_port = 12346;
size_t num_queries = 2000;

class LSpec {
public:
	LSpec(const QuerySpec &spec,uint32_t tiles,uint32_t res) :
		spec(spec),tiles(tiles),res(res) {}
	const QuerySpec &spec;
	uint32_t tiles;
	uint32_t res;
};


void execute( LSpec &s ) {

	int num_threads = num_nodes * workers_per_node * 2;

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

	ClientCacheManager ccm("127.0.0.1",index_port);
	ParallelExecutor pe(queries,ccm,num_threads);

	CacheExperiment::TimePoint start, end;
	start = CacheExperiment::SysClock::now();
	pe.execute();
	end = CacheExperiment::SysClock::now();
	printf("Execution of %ld queries took: %ldms\n", num_queries, CacheExperiment::duration(start,end) );
}

void termination_handler(int signum) {
	if ( signum == SIGSEGV ) {
		printf("Segmentation fault. Stacktrace:\n%s", CacheCommon::get_stacktrace().c_str());
		exit(1);
	}
}

void set_signal_handler() {
	struct sigaction new_action, old_action;

	/* Set up the structure to specify the new action. */
	new_action.sa_handler = termination_handler;
	sigemptyset(&new_action.sa_mask);
	new_action.sa_flags = 0;

	sigaction(SIGINT, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGINT, &new_action, NULL);
	sigaction(SIGHUP, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGHUP, &new_action, NULL);
	sigaction(SIGTERM, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGTERM, &new_action, NULL);
	sigaction(SIGSEGV, NULL, &old_action);
		sigaction(SIGSEGV, &new_action, NULL);
}

int main(void) {
	CacheCommon::set_uncaught_exception_handler();
	set_signal_handler();
	Configuration::loadFromDefaultPaths();

	// Disable GDAL Error Messages
	CPLSetErrorHandler(CacheCommon::GDALErrorHandler);

#ifndef MAPPING_NO_OPENCL
	RasterOpenCL::init();
#endif
	CachingStrategy::init();

	Log::setLevel(Log::LogLevel::WARN);

	size_t cache_capacity = 50 * 1024 * 1024;

	std::string reorg_strategy = "geo";
	std::string relevance = "costlru";
	std::string caching_strat = "uncached";

	std::string scheduler = "default";
	std::string node_cache_mode = "remote";
	std::string node_cache_repl = "costlru";
	time_t update_interval = 500;

//	std::string in;

	{
		LocalTestSetup lts(
				num_nodes, workers_per_node, update_interval, cache_capacity, reorg_strategy, relevance, caching_strat, scheduler, node_cache_mode, node_cache_repl, index_port
		);

		LSpec ls( cache_exp::srtm, 16, 256 );
		execute( ls );
		lts.get_index().force_stat_update();
//		std::cin >> in;
	}
//	std::cin >> in;
	Log::warn("DONE!");
	return 0;
}



