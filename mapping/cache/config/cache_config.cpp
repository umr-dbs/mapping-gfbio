/*
 * cache_config.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache_config.h"


std::unique_ptr<CachingStrategy> CacheConfig::cs = std::unique_ptr<CachingStrategy>( new CacheAll() );

const CachingStrategy& CacheConfig::get_caching_strategy() {
	return *cs;
}


