/*
 * cache_config.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef CACHE_CONFIG_H_
#define CACHE_CONFIG_H_

#include "cache/config/caching_strategy.h"
#include <memory>

class CacheConfig {
public:
	static const CachingStrategy& get_caching_strategy();
private:
	static std::unique_ptr<CachingStrategy> cs;
};

#endif /* CACHE_CONFIG_H_ */
