/*
 * local_node_manager.h
 *
 *  Created on: 23.05.2016
 *      Author: koerberm
 */

#ifndef CACHE_LOCAL_MANAGER_H_
#define CACHE_LOCAL_MANAGER_H_

#include "cache/node/node_manager.h"
#include "cache/node/manager/local_replacement.h"
#include "cache/node/puzzle_util.h"


class LocalCacheManager;

/**
 * Local implementation of cache manager
 */
template<class T>
class LocalCacheWrapper : public NodeCacheWrapper<T> {
	friend class LocalCacheManager;
public:
	LocalCacheWrapper( LocalCacheManager &mgr, const std::string &repl, size_t size, CacheType type );
	bool put(const std::string &semantic_id, const std::unique_ptr<T> &item, const QueryRectangle &query, const QueryProfiler &profiler);
	std::unique_ptr<T> query(GenericOperator &op, const QueryRectangle &rect, QueryProfiler &profiler);
	std::unique_ptr<T> process_puzzle( const PuzzleRequest& request, QueryProfiler &parent_profiler );
	MetaCacheEntry put_local(const std::string &semantic_id, const std::unique_ptr<T> &item, CacheEntry &&info );
	void remove_local(const NodeCacheKey &key);
private:
	std::mutex rem_mtx;
	LocalCacheManager &mgr;
	std::unique_ptr<LocalReplacement<T>> replacement;
};


class LocalCacheManager : public NodeCacheManager {
public:
	LocalCacheManager( const std::string &strategy, const std::string &replacement,
			size_t raster_cache_size, size_t point_cache_size, size_t line_cache_size,
			size_t polygon_cache_size, size_t plot_cache_size );
};


#endif
