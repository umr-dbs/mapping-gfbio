/*
 * server_main.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include "cache/server.h"
#include "util/configuration.h"
#include <signal.h>

CacheServer *cs = nullptr;

void termHandler( int signum )
{
	if ( cs != nullptr )
		cs->stop();
}

int main(void) {
	signal(SIGTERM, termHandler);
	signal(SIGINT, termHandler);
	Configuration::loadFromDefaultPaths();
	auto portstr = Configuration::get("cacheserver.port");
	auto portnr = atoi(portstr.c_str());
	auto numThreadsstr = Configuration::get("cacheserver.threads","4");

	// Inititalize cache
	if ( Configuration::getBool("cache.enabled",false) ) {
		size_t raster_size = atoi( Configuration::get("cache.raster.size", "5242880" ).c_str() );
		//std::unique_ptr<CacheManager> nopImpl( new NopCacheManager() );
		std::unique_ptr<CacheManager> defImpl( new DefaultCacheManager( raster_size ) );
		CacheManager::init( defImpl );
	}
	else {
		std::unique_ptr<CacheManager> nopImpl( new NopCacheManager() );
		CacheManager::init( nopImpl );
	}

	auto numThreads = atoi(numThreadsstr.c_str());
	cs = new CacheServer(portnr, numThreads);
	cs->run();
	return 0;
}


