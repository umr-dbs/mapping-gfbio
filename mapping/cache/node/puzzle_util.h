/*
 * puzzle_util.h
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */

#ifndef NODE_PUZZLE_UTIL_H_
#define NODE_PUZZLE_UTIL_H_

#include "cache/node/node_manager.h"
#include "cache/priv/requests.h"
#include "operators/operator.h"

class PuzzleJob {
	template <class T> friend class LocalCacheWrapper;
public:
	template<class T>
	static std::unique_ptr<T> process(GenericOperator &op,
			const QueryRectangle &query, const std::vector<Cube<3>> &remainder,
			const std::vector<std::shared_ptr<const T>> &parts,
			QueryProfiler &profiler);
private:

	/**
	 * Enlarges the result of the puzzle-request to the maximum bounding cube.
	 * @param query the query-rectangle of the request
	 * @param items the puzzle-pieces
	 * @return the maximum bounding box of the result
	 */
	template<class T>
	static SpatioTemporalReference enlarge_puzzle(const QueryRectangle &query,
			const std::vector<std::shared_ptr<const T>>& items);

	/**
	 * Conmputes the remainder-queries
	 * @param semantic_id the semantic id
	 * @param ref_result a result used as reference for resolution computation
	 * @param request the puzzle-request
	 * @param qp the profiler to use
	 * @return the results of the remainder queries
	 */
	template<class T>
	static std::vector<std::unique_ptr<T>> compute_remainders(
			const QueryRectangle& query, GenericOperator &op, const T& ref_result,
			const std::vector<Cube<3> >& remainder, QueryProfiler& profiler);

	template<class T>
	static std::vector<QueryRectangle> get_remainder_queries(
			const QueryRectangle& query, const std::vector<Cube<3> >& remainder, const T& ref_result);

	/**
	 * Snaps the bounds of a cube to the pixel grid
	 * @param v1 the first value of the interval
	 * @param v2 the second value of the interval
	 * @param ref the reference point (value where a full pixel starts)
	 * @param scale the pixel-scale
	 *
	 */
	static void snap_to_pixel_grid(double &v1, double &v2, double ref,
			double scale);

	template<class T>
	static std::unique_ptr<T> compute(GenericOperator &op,
			const QueryRectangle &query, QueryProfiler &qp);

	/**
	 * Puzzles the given items into a result with the dimensions of bbox.
	 * @param bbox the bounding box of the result
	 * @param items the puzzle-pieces
	 * @return the combined result
	 */
	template<class T>
	static std::unique_ptr<T> puzzle(const SpatioTemporalReference &bbox,
			const std::vector<std::shared_ptr<const T>> &items);

	template<class T>
	static std::unique_ptr<T> puzzle_feature_collection(
			const SpatioTemporalReference &bbox,
			const std::vector<std::shared_ptr<const T>> &items);

	template<class T>
	static void append_idxs(T &dest, const T &src);

	/**
	 * Helper for child-classes to append a vector of indices
	 * @param dest the target vector
	 * @param src the source vector
	 */
	static void append_idx_vec(std::vector<uint32_t> &dest,
			const std::vector<uint32_t> &src);
};

/**
 * Interface to determine whether a CacheRef points to the local cache
 * or a foreign node
 */
class CacheRefHandler {
public:
	virtual ~CacheRefHandler() = default;
	/**
	 * @param id the id to create a local reference for
	 * @return a reference to the local cache-entry with the given id
	 */
	virtual CacheRef create_local_ref(uint64_t id) const = 0;
	/**
	 * @param ref the CacheRef to check
	 * @return whether the given ref points to a local cache entry
	 */
	virtual bool is_local_ref(const CacheRef& ref) const = 0;
};

/**
 * Interface for retrieving cache entries
 */
template<class T>
class PieceRetriever {
public:
	virtual ~PieceRetriever() = default;
	/**
	 * Fetches the cache entry pointed to by the given semantic id and CacheRef
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item fetched
	 */
	virtual std::shared_ptr<const T> fetch(const std::string &semantic_id,
			const CacheRef &ref, QueryProfiler &qp) const = 0;

};

/**
 * Retriever implementation for local usage.
 */
template<class T>
class LocalRetriever: public PieceRetriever<T> {
public:
	/**
	 * Constructs a new instance for the given cache
	 * @param cache the underlying cache
	 */
	LocalRetriever(const NodeCacheWrapper<T> &cache);
	virtual ~LocalRetriever() = default;

	/**
	 * Fetches the cache entry pointed to by the given semantic id and CacheRef
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item fetched
	 */
	virtual std::shared_ptr<const T> fetch(const std::string &semantic_id,
			const CacheRef &ref, QueryProfiler &qp) const;

protected:
	const NodeCacheWrapper<T> &cache;
};

/**
 * Retriever implementation combining the local and global cache
 */
template<class T>
class RemoteRetriever: public LocalRetriever<T> {
public:
	/**
	 * Constructs a new instance for the given cache and handler
	 * @param cache the underlying cache
	 * @param handler the CacheRef handler
	 */
	RemoteRetriever(const NodeCacheWrapper<T> &cache,
			const CacheRefHandler &handler);

	/**
	 * Fetches the cache entry pointed to by the given semantic id and CacheRef.
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item fetched
	 */
	std::shared_ptr<const T> fetch(const std::string &semantic_id,
			const CacheRef &ref, QueryProfiler &qp) const;

	/**
	 * Loads the desired item from a foreign node
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item retrieved
	 */
	std::unique_ptr<T> load(const std::string &sematic_id, const CacheRef &ref,
			QueryProfiler &qp) const;
private:
	/**
	 * Reads an data-item from the given buffer
	 * @param buffer the buffer to read the item from
	 */
	std::unique_ptr<T> read_item(BinaryReadBuffer &buffer) const;
	const CacheRefHandler &ref_handler;
};

#endif /* NODE_PUZZLE_UTIL_H_ */
