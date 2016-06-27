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
	struct sigaction new_action, old_action, ignore;

	/* Set up the structure to specify the new action. */
	new_action.sa_handler = termination_handler;
	sigemptyset(&new_action.sa_mask);
	new_action.sa_flags = 0;

	ignore.sa_handler = SIG_IGN;
	sigemptyset(&ignore.sa_mask);
	ignore.sa_flags = 0;

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

	sigaction(SIGPIPE, NULL, &old_action);
		sigaction(SIGPIPE,&ignore,NULL);

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

	size_t capacity = 50 * 1024 * 1024;

	NodeConfig ncfg;
	ncfg.index_host = "127.0.0.1";
	ncfg.index_port = index_port;
	ncfg.delivery_port = 12347;
	ncfg.num_workers = workers_per_node;
	ncfg.raster_size = capacity;
	ncfg.point_size = capacity;
	ncfg.line_size = capacity;
	ncfg.polygon_size = capacity;
	ncfg.plot_size = capacity;
	ncfg.mgr_impl = "remote";
	ncfg.local_replacement = "lru";
	ncfg.caching_strategy = "uncached";

	IndexConfig icfg;
	icfg.port = index_port;
	icfg.update_interval = 500;
	icfg.scheduler = "default";
	icfg.reorg_strategy = "geo";
	icfg.relevance_function = "costlru";
	icfg.batching_enabled = false;

	LocalTestSetup lts(num_nodes, ncfg, icfg);
	std::this_thread::sleep_for(std::chrono::seconds(3600));
	return 0;
}



