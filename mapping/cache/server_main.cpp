/*
 * server_main.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include "cache/server.h"
#include "cache/cache.h"
#include "util/configuration.h"
#include <signal.h>
#include <memory>

CacheServer *cs = nullptr;

void termination_handler(int signum) {
	cs->stop();
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
}

int main(void) {
	set_signal_handler();
	Configuration::loadFromDefaultPaths();
	auto portstr = Configuration::get("cacheserver.port");
	auto portnr = atoi(portstr.c_str());
	auto numThreadsstr = Configuration::get("cacheserver.threads", "4");

	// Inititalize cache
	if (Configuration::getBool("cache.enabled", false)) {
		size_t raster_size = atoi(
				Configuration::get("cache.raster.size", "5242880").c_str());
		//std::unique_ptr<CacheManager> nopImpl( new NopCacheManager() );
		std::unique_ptr<CacheManager> defImpl(
				new DefaultCacheManager(raster_size));
		CacheManager::init(defImpl);
	} else {
		std::unique_ptr<CacheManager> nopImpl(new NopCacheManager());
		CacheManager::init(nopImpl);
	}

	auto numThreads = atoi(numThreadsstr.c_str());
	cs = new CacheServer(portnr, numThreads);
	cs->run();
	return 0;
}

