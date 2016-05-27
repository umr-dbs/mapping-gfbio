/*
 * local_replacement.h
 *
 *  Created on: 24.05.2016
 *      Author: koerberm
 */

#ifndef CACHE_NODE_MANAGER_LOCAL_REPLACEMENT_H_
#define CACHE_NODE_MANAGER_LOCAL_REPLACEMENT_H_

#include "cache/node/node_cache.h"
#include <algorithm>

class LocalRef: public NodeCacheKey, public CacheEntry {
public:
	LocalRef(const std::string &semantic_id, const HandshakeEntry &e) :
			NodeCacheKey(semantic_id, e.entry_id), CacheEntry(e) {
	}
};

template<class T>
class LocalReplacement {
public:
	static std::unique_ptr<LocalReplacement<T>> by_name( const std::string &name );

	virtual ~LocalReplacement() = default;
	std::vector<LocalRef> get_removals(NodeCache<T> &cache,
			size_t space_required);
protected:
	virtual std::vector<LocalRef> compute_removals(
			std::vector<LocalRef> &all_entries, size_t space_required) = 0;
};


template<class T>
std::vector<LocalRef> LocalReplacement<T>::get_removals(NodeCache<T>& cache,
		size_t space_required) {
	std::vector<LocalRef> all_entries;

	size_t avail = cache.get_max_size() - cache.get_current_size();
	if ( avail < space_required ) {
		CacheHandshake hs = cache.get_all();
		for (auto &p : hs.get_items()) {
			for (auto &e : p.second)
				all_entries.push_back(LocalRef(p.first, e));
		}
		return compute_removals(all_entries, space_required-avail);
	}
	return all_entries;
}

template<class T>
class LRUReplacement: public LocalReplacement<T> {
protected:
	static bool lru_comp( const LocalRef &e1, const LocalRef &e2 );

	std::vector<LocalRef> compute_removals(std::vector<LocalRef> &all_entries, size_t space_required);
};

template<class T>
bool LRUReplacement<T>::lru_comp(const LocalRef& e1,
		const LocalRef& e2) {
	return e1.last_access < e2.last_access;
}

template<class T>
std::vector<LocalRef> LRUReplacement<T>::compute_removals(
		std::vector<LocalRef>& all_entries, size_t space_required) {

	std::vector<LocalRef> result;

	std::sort(all_entries.begin(), all_entries.end(), LRUReplacement::lru_comp);

	size_t space_freed = 0;
	auto i = all_entries.begin();

	while ( space_freed < space_required && i != all_entries.end() ) {
		space_freed += i->size;
		result.push_back( *i );
		i++;
	}
	return result;
}

template<class T>
std::unique_ptr<LocalReplacement<T> > LocalReplacement<T>::by_name(
		const std::string& name) {

	std::string lc;
	lc.resize(name.size());
	std::transform(name.cbegin(),name.cend(),lc.begin(),::tolower);

	if ( lc == "lru" )
		return make_unique<LRUReplacement<T>>();
	throw ArgumentException(concat("Unknown replacement: ", name));

}

#endif /* CACHE_NODE_MANAGER_LOCAL_REPLACEMENT_H_ */
