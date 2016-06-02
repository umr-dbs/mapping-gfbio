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


/**
 * Defines an ordering on cache-entries, so
 * that after sorting them, the least relevant entries
 * are at the end of the sorted structure
 */
class LocalRelevanceFunction {
public:
	static std::unique_ptr<LocalRelevanceFunction> by_name( const std::string &name );
	virtual ~LocalRelevanceFunction() = default;
	virtual void new_turn();
	/**
	 * Compares the given entries by their relevance. If e1 is more relevant
	 * than e2, true is returned.
	 * @param e1 the first entry
	 * @param e2 the second entry
	 * @return whether e1 is more relevant than e2
	 */
	virtual bool operator() ( const LocalRef &e1, const LocalRef &e2 ) const = 0;
};

/**
 * Simple LRU-Implementation of the relevance function
 */
class LocalLRU : public LocalRelevanceFunction {
public:
	bool operator() ( const LocalRef &e1, const LocalRef &e2 ) const;
};

/**
 * A cost based LRU implementation of the relevance function.
 * Main factor for the ordering are the computation costs. They are
 * weighted with a time since the last access to the entry.
 */
class LocalCostLRU : public LocalRelevanceFunction {
public:
	LocalCostLRU();
	void new_turn();
	bool operator() ( const LocalRef &e1, const LocalRef &e2 ) const;
private:
	time_t now;
};

template<class T>
class LocalReplacement {
public:
	LocalReplacement( std::unique_ptr<LocalRelevanceFunction> relevance );
	std::vector<LocalRef> get_removals(NodeCache<T> &cache, size_t space_required);
private:
	std::vector<LocalRef> compute_removals(std::vector<LocalRef> &all_entries, size_t space_required);
	std::unique_ptr<LocalRelevanceFunction> relevance;
};

#endif /* CACHE_NODE_MANAGER_LOCAL_REPLACEMENT_H_ */
