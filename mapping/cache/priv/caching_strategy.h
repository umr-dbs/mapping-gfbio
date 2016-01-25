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
	enum class Type { SELF, ALL, UNCACHED };
	static void init();
	static double get_costs( const ProfilingData &profile, Type type );
	static double get_caching_costs( size_t bytes );
private:
	static double caching_time( uint32_t w,uint32_t h);
	static double fixed_caching_time;
	static double caching_time_per_byte;

public:
	static std::unique_ptr<CachingStrategy> by_name( const std::string &name );
	virtual ~CachingStrategy() = default;
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const = 0;
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
class SimpleThresholdStrategy : public CachingStrategy {
public:
	SimpleThresholdStrategy( Type type );
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
private:
	Type   type;
};

#endif /* CACHING_STRATEGY_H_ */
