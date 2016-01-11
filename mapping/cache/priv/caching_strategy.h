/*
 * caching_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef CACHING_STRATEGY_H_
#define CACHING_STRATEGY_H_

#include "operators/queryprofiler.h"
#include <memory>

//
// The caching-strategy tells whether or not to cache
// the result of a computation.
// It uses the profiler-data and the result-size in bytes
//

class CachingStrategy {
public:
	static std::unique_ptr<CachingStrategy> by_name( const std::string &name );
	virtual ~CachingStrategy() = default;
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const = 0;
	double get_costs( const QueryProfiler &profiler, size_t bytes, bool use_all = false ) const;
};

//
// Caches all results
//
class CacheAll : public CachingStrategy {
public:
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

//
// Never caches a result
//
class CacheNone : public CachingStrategy {
public:
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

//
// Strategy employed by christian authmann
//
class AuthmannStrategy : public CachingStrategy {
public:
	AuthmannStrategy( double threshold );
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
private:
	double threshold;
};

//
// Two step strategy:
// - First checks if the computation was that expensive, that the result should be cached anyway
// - If not, checks if there have been numerous computations without caching a result which stack
//   to a cache-worthy computation time
//
class TwoStepStrategy : public CachingStrategy {
public:
	// TODO: Figure out good values!
	TwoStepStrategy(double stacked_threshold = 3, double immediate_threshold = 2, uint stack_depth = 3);
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
private:
	const double stacked_threshold;
	const double immediate_threshold;
	const uint stack_depth;
};

#endif /* CACHING_STRATEGY_H_ */
