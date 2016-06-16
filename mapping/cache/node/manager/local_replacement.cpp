/*
 * local_replacement.cpp
 *
 *  Created on: 01.06.2016
 *      Author: koerberm
 */

#include "cache/node/manager/local_replacement.h"
#include "cache/priv/caching_strategy.h"
#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

std::unique_ptr<LocalRelevanceFunction> LocalRelevanceFunction::by_name(
		const std::string& name) {

	std::string lc;
	lc.resize(name.size());
	std::transform(name.cbegin(),name.cend(),lc.begin(),::tolower);

	if ( lc == "lru" )
		return make_unique<LocalLRU>();
	else if ( lc == "costlru" )
		return make_unique<LocalCostLRU>();
	throw ArgumentException(concat("Unknown replacement: ", name));

}

void LocalRelevanceFunction::new_turn() {
}

bool LocalLRU::operator ()(const LocalRef& e1, const LocalRef& e2) const  {
	return e1.last_access < e2.last_access;
}

LocalCostLRU::LocalCostLRU() : now(0) {}

void LocalCostLRU::new_turn()  {
	now = CacheCommon::time_millis();
}

bool LocalCostLRU::operator ()(const LocalRef& e1, const LocalRef& e2) const  {
	double f1 = 1.0 - (((now - e1.last_access) / 60000) * 0.01);
	double f2 = 1.0 - (((now - e2.last_access) / 60000) * 0.01);

	double c1 = CachingStrategy::get_costs(e1.profile,CachingStrategy::Type::UNCACHED);
	double c2 = CachingStrategy::get_costs(e2.profile,CachingStrategy::Type::UNCACHED);

	return (c1 * f1) < (c2 * f2);
}

//
// IMPL
//
template<class T>
LocalReplacement<T>::LocalReplacement(std::unique_ptr<LocalRelevanceFunction> relevance) : relevance(std::move(relevance)) {
}

template<class T>
std::vector<LocalRef> LocalReplacement<T>::get_removals(NodeCache<T>& cache,
		size_t space_required) {
	std::vector<LocalRef> all_entries;

	size_t avail = (cache.get_current_size() > cache.get_max_size()) ? 0 : cache.get_max_size() - cache.get_current_size();
	if ( avail < space_required ) {
		CacheHandshake hs = cache.get_all();
		for (auto &p : hs.get_items()) {
			for (auto &e : p.second)
				all_entries.push_back(LocalRef(p.first, e));
		}
		relevance->new_turn();
		return compute_removals(all_entries, space_required-avail);
	}
	return all_entries;
}

template<class T>
std::vector<LocalRef> LocalReplacement<T>::compute_removals(
		std::vector<LocalRef>& all_entries, size_t space_required) {

	std::vector<LocalRef> result;
	std::sort(all_entries.begin(), all_entries.end(), std::ref(*relevance) );

	size_t space_freed = 0;
	auto i = all_entries.begin();

	while ( space_freed < space_required && i != all_entries.end() ) {
		space_freed += i->size;
		result.push_back( *i );
		i++;
	}
	return result;
}

template class LocalReplacement<GenericRaster>;
template class LocalReplacement<PointCollection>;
template class LocalReplacement<LineCollection>;
template class LocalReplacement<PolygonCollection>;
template class LocalReplacement<GenericPlot> ;
