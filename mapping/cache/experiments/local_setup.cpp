#include "cache/experiments/exp_util.h"
#include "cache/experiments/exp_workflows.h"
#include "util/configuration.h"
#include "util/log.h"
#include "raster/opencl.h"
#include <signal.h>

int num_nodes = 10;
int workers_per_node = 4;
int index_port = 12346;

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

	Log::setLevel(Log::LogLevel::INFO);

	size_t cache_capacity = 50 * 1024 * 1024;

	std::string reorg_strategy = "geo";
	std::string relevance = "costlru";
	std::string caching_strat = "uncached";

	std::string scheduler = "late";
	bool batching = true;
	std::string node_cache_mode = "remote";
	std::string node_cache_repl = "lru";
	time_t update_interval = 500;

	LocalTestSetup lts(
			num_nodes, workers_per_node, update_interval, cache_capacity, reorg_strategy, relevance, caching_strat, scheduler, batching, node_cache_mode, node_cache_repl, index_port
	);
	std::this_thread::sleep_for(std::chrono::seconds(3600));
	return 0;
}



