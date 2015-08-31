/*
 * caching_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef CACHING_STRATEGY_H_
#define CACHING_STRATEGY_H_

#include "operators/queryprofiler.h"

//
// The caching-strategy tells whether or not to cache
// the result of a computation.
// It uses the profiler-data and the result-size in bytes
//

class CachingStrategy {
public:
	CachingStrategy();
	virtual ~CachingStrategy();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const = 0;
};

//
// Caches all results
//
class CacheAll : public CachingStrategy {
public:
	CacheAll();
	virtual ~CacheAll();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

//
// Strategy employed by christian authmann
//
class AuthmannStrategy : public CachingStrategy {
public:
	AuthmannStrategy();
	virtual ~AuthmannStrategy();
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

//
// Two step strategy:
// - First checks if the computation was that expensive, that the result should be cached anyway
// - If not, checks if there have been numerous computations without caching a result which stack
//   to a cache-worthy computation time
//
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
