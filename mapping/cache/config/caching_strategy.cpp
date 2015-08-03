/*
 * caching_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "caching_strategy.h"

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

bool CacheAll::do_cache(const QueryProfiler& profiler) const {
	(void) profiler;
	return true;
}

//
// Simple Heuristik
//
