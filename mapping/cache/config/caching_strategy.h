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
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const = 0;
};

class CacheAll : public CachingStrategy {
public:
	CacheAll();
	virtual ~CacheAll();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

class AuthmannStrategy : public CachingStrategy {
public:
	AuthmannStrategy();
	virtual ~AuthmannStrategy();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

class TwoStepStrategy : public CachingStrategy {
public:
	TwoStepStrategy(double stacked_threshold, double immediate_threshold, uint stack_depth);
	virtual ~TwoStepStrategy();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
private:
	const double stacked_threshold;
	const double immediate_threshold;
	const uint stack_depth;
};

#endif /* CACHING_STRATEGY_H_ */
