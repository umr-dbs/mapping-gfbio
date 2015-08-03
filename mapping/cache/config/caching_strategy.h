/*
 * caching_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef CACHING_STRATEGY_H_
#define CACHING_STRATEGY_H_

#include "operators/queryprofiler.h"

class CachingStrategy {
public:
	CachingStrategy();
	virtual ~CachingStrategy();
	virtual bool do_cache( const QueryProfiler &profiler ) const = 0;
};

class CacheAll : public CachingStrategy {
public:
	CacheAll();
	virtual ~CacheAll();
	virtual bool do_cache( const QueryProfiler &profiler ) const;
};

#endif /* CACHING_STRATEGY_H_ */
