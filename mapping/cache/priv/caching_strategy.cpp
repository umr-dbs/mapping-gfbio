/*
 * caching_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache/priv/caching_strategy.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/concat.h"

std::unique_ptr<CachingStrategy> CachingStrategy::by_name(const std::string& name) {
	if ( name == "never")
		return make_unique<CacheNone>();
	else if ( name == "always")
		return make_unique<CacheAll>();
	else if ( name == "simple")
		return make_unique<AuthmannStrategy>();
	else if ( name == "twostep")
		return make_unique<TwoStepStrategy>();
	throw ArgumentException(concat("Unknown Caching-Strategy: ", name));
}

double CachingStrategy::get_costs(const QueryProfiler& profiler, size_t bytes) const {
	return 0.0;
}

//
// Cache All
//

bool CacheAll::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	(void) profiler;
	(void) bytes;
	return true;
}

//
// Cache None
//

bool CacheNone::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	(void) profiler;
	(void) bytes;
	return false;
}

//
// Authmann Heuristik
//

bool AuthmannStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	double cache_cpu = 0.000000005 * bytes;
	return bytes > 0 &&
			(2 * cache_cpu < (profiler.all_cpu + profiler.all_gpu) ||
			 2 * bytes < profiler.all_io );
}

//
// 2-Step-strategy
//

TwoStepStrategy::TwoStepStrategy(double stacked_threshold, double immediate_threshold, uint stack_depth) :
  stacked_threshold(stacked_threshold), immediate_threshold(immediate_threshold), stack_depth(stack_depth) {
}

bool TwoStepStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	double v = profiler.self_cpu + profiler.self_gpu + profiler.self_io * 0.0000001 + 0.000000005 * bytes;
	double av = profiler.all_cpu + profiler.all_gpu + profiler.all_io * 0.0000001 + 0.000000005 * bytes;
	return v >= immediate_threshold ||
		   (profiler.uncached_depth >= stack_depth && av >= stacked_threshold);
}

