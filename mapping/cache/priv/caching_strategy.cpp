/*
 * caching_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache/priv/caching_strategy.h"
#include "cache/node/node_cache.h"
#include "util/exceptions.h"
#include "util/make_unique.h"
#include "util/concat.h"


std::unique_ptr<CachingStrategy> CachingStrategy::by_name(const std::string& name) {
	if ( name == "never")
		return make_unique<CacheNone>();
	else if ( name == "always")
		return make_unique<CacheAll>();
	else if ( name == "self")
		return make_unique<SimpleThresholdStrategy>(Type::SELF);
	else if ( name == "uncached")
			return make_unique<SimpleThresholdStrategy>(Type::UNCACHED);
	throw ArgumentException(concat("Unknown Caching-Strategy: ", name));
}

double CachingStrategy::fixed_caching_time(0);
double CachingStrategy::caching_time_per_byte(0);

void CachingStrategy::init() {
	// Calibrate cache costs
	fixed_caching_time    = caching_time(1,1);
	caching_time_per_byte = (caching_time(3072,3072) - fixed_caching_time) / (3072*3072);
}

double CachingStrategy::get_caching_costs(size_t bytes) {
	return fixed_caching_time + (bytes*caching_time_per_byte);
}


double CachingStrategy::caching_time(uint32_t w, uint32_t h) {
	int num_runs = 10;
	NodeCache<GenericRaster> nc(CacheType::RASTER, 50000000);
	QueryProfiler qp;
	for ( int i = 0; i < num_runs; i++ ) {
		DataDescription dd(GDT_Byte,Unit::unknown());
		SpatioTemporalReference stref(
			SpatialReference::extent(EPSG_WEBMERCATOR),
			TemporalReference(TIMETYPE_UNIX,i,i+1)
		);
		auto raster = GenericRaster::create(dd,stref,w,h);
		qp.startTimer();
		auto res = nc.put("test",raster, CacheEntry(CacheCube(*raster),w*h,ProfilingData()));
		qp.stopTimer();
	}
	return qp.self_cpu / num_runs;
}

double CachingStrategy::get_costs(const ProfilingData& profile, Type type) {
	double io  = 0;
	double cpu = 0;
	double gpu = 0;

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

	double time_fact = cpu + gpu;
	// Assume 40MB /sec
	double io_fact = io / 1024 / 1024 / 40;
	return io_fact + time_fact;
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
// Threshold strategy
//

SimpleThresholdStrategy::SimpleThresholdStrategy(Type type) :
	type(type) {
}

bool SimpleThresholdStrategy::do_cache(const QueryProfiler& profiler, size_t bytes) const {
	// Assume 1 put and at least 2 gets
	return get_costs(profiler,type) >= 3.5 * get_caching_costs(bytes);
}
