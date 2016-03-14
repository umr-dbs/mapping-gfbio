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


/**
 * The CachingStrategy defines which computation-results
 * are stored in the cache
 */
class CachingStrategy {
public:
	/**
	 * Defines which cost-profile is used
	 * <ul>
	 * <li><b>SELF</b> - computational costs of the actual operator</li>
	 * <li><b>ALL</b> - computational costs of the whole operator tree (even with costs of already cached results)</li>
	 * <li><b>UNCACHED</b> - computational costs which are not covered by an cache-entry. Including the actual operator and all of its childrean</li>
	 * </ul>
	 */
	enum class Type { SELF, ALL, UNCACHED };

	/**
	 * Inititializes the cost model by storing data of different size in the cache
	 */
	static void init();

	/**
	 * Retrieves the caching strategy for the given name
	 * @param name the name of the strategy
	 * @return the strategy
	 */
	static std::unique_ptr<CachingStrategy> by_name( const std::string &name );

	/**
	 * Calculates the cost-factor from the given profile and type
	 * @param profile the cost-profile
	 * @param type the type of the profile
	 * @return the cost-factor for the given type and profile
	 */
	static double get_costs( const ProfilingData &profile, Type type );

	/**
	 * Tells the costs of caching an entry with the given size in bytes
	 * @param bytes the size of the entry in bytes
	 * @return the cost-factor for caching the entry
	 */
	static double get_caching_costs( size_t bytes );
private:

	/**
	 * Helper used in initialization determining the costs
	 * for caching rasters of the given with and height
	 * @param w the height of the rasters to use
	 * @param w the width of the rasters to use
	 * @return the cpu-time used for caching a raster of the given size
	 */
	static double caching_time( uint32_t w,uint32_t h);

	/** Holds the fix costs for caching a results */
	static double fixed_caching_time;
	/** Holds the costs per byte for caching a result */
	static double caching_time_per_byte;

public:
	virtual ~CachingStrategy() = default;

	/**
	 * Tells whether the result with the given size and cost-profile should be cached
	 * @param profiler the profiler used to track computation costs
	 * @param size the size of the result in bytes
	 */
	virtual bool do_cache( const QueryProfiler &profiler, size_t bytes ) const = 0;
};

/**
 * This strategy caches all entries
 */
class CacheAll : public CachingStrategy {
public:
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

/**
 * This strategy never caches an entries
 */
class CacheNone : public CachingStrategy {
public:
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
};

/**
 * This strategy caches entries if their computational costs
 * are 3 times greater than the costs of caching the result.
 * This ensures, that the cache is not flooded with simple computations.
 */
class SimpleThresholdStrategy : public CachingStrategy {
public:
	SimpleThresholdStrategy( Type type );
	bool do_cache( const QueryProfiler &profiler, size_t bytes ) const;
private:
	/** The cost-profile to use for evaluation */
	Type   type;
};

#endif /* CACHING_STRATEGY_H_ */
