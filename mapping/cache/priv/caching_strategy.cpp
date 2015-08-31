/*
 * caching_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache/priv/caching_strategy.h"

CachingStrategy::CachingStrategy() {
}

CachingStrategy::~CachingStrategy() {
}

//
// Cache All
//

CacheAll::CacheAll() {
}

CacheAll::~CacheAll() {
}

bool CacheAll::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	(void) profiler;
	(void) bytes;
	return true;
}

AuthmannStrategy::AuthmannStrategy() {
}

AuthmannStrategy::~AuthmannStrategy() {
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

TwoStepStrategy::~TwoStepStrategy() {
}

bool TwoStepStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	double v = profiler.self_cpu + profiler.self_gpu + profiler.self_io * 0.0000001 + 0.000000005 * bytes;
	double av = profiler.all_cpu + profiler.all_gpu + profiler.all_io * 0.0000001 + 0.000000005 * bytes;
	return v >= immediate_threshold ||
		   (profiler.uncached_depth >= stack_depth && av >= stacked_threshold);
}

