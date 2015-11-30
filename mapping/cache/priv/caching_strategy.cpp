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

double CachingStrategy::get_costs(const QueryProfiler& profiler, size_t bytes, bool use_all) const {
	double io = use_all ? profiler.all_io : profiler.self_io;
	double proc = use_all ? (profiler.all_cpu + profiler.all_gpu) : (profiler.self_cpu + profiler.self_gpu);
	// TODO: Check this factor;
	double cache_cpu = 0.000000005 * bytes;

	return std::max(
		io / (double) bytes,
		proc / cache_cpu
	);
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
	return get_costs(profiler, bytes, true) > 2;
}

//
// 2-Step-strategy
//

TwoStepStrategy::TwoStepStrategy(double stacked_threshold, double immediate_threshold, uint stack_depth) :
  stacked_threshold(stacked_threshold), immediate_threshold(immediate_threshold), stack_depth(stack_depth) {
}

bool TwoStepStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	return get_costs(profiler,bytes, false) > immediate_threshold ||
		   (profiler.uncached_depth >= stack_depth && get_costs(profiler,bytes,true) > stacked_threshold );
}

