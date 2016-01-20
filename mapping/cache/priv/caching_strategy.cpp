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
		return make_unique<AuthmannStrategy>(2);
	else if ( name == "twostep")
		return make_unique<TwoStepStrategy>();
	throw ArgumentException(concat("Unknown Caching-Strategy: ", name));
}

double CachingStrategy::get_costs(const ProfilingData& profile, size_t bytes, Type type) {
	double io;
	double cpu;
	double gpu;

	switch ( type ) {
	case Type::SELF:
		cpu = profile.self_cpu;
		gpu = profile.self_gpu;
		io = profile.self_io;
		break;
	case Type::ALL:
		cpu = profile.all_cpu;
		gpu = profile.all_gpu;
		io = profile.all_io;
		break;
	case Type::UNCACHED:
		cpu = profile.uncached_cpu;
		gpu = profile.uncached_gpu;
		io = profile.uncached_io;
		break;
	}

	double proc = cpu + gpu;
	// TODO: Check this factor;
	double cache_cpu = 0.000000005 * bytes;

	double res = io / (double) bytes + proc / cache_cpu;

	return res;
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

AuthmannStrategy::AuthmannStrategy(double threshold) : threshold(threshold) {
}

bool AuthmannStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	return get_costs(profiler, bytes, Type::UNCACHED) >= threshold;
}

//
// 2-Step-strategy
//

TwoStepStrategy::TwoStepStrategy(double stacked_threshold, double immediate_threshold) :
  stacked_threshold(stacked_threshold), immediate_threshold(immediate_threshold) {
}

bool TwoStepStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	return get_costs(profiler,bytes, Type::SELF    ) >= immediate_threshold ||
		   get_costs(profiler,bytes, Type::UNCACHED) >= stacked_threshold;
}
