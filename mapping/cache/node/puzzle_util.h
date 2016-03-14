/*
 * puzzle_util.h
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */

#ifndef NODE_PUZZLE_UTIL_H_
#define NODE_PUZZLE_UTIL_H_

#include "cache/node/node_cache.h"
#include "cache/priv/requests.h"
#include "operators/operator.h"


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
	virtual std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const = 0;

	/**
	 * Computes the item described by the given operator tree and query rectangle
	 * @param op the operator tree
	 * @param query the query's bounds
	 * @param qp the profiler to use
	 * @return the computed result
	 */
	virtual std::unique_ptr<T> compute( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) const = 0;
};

/**
 * Retriever implementation for local usage.
 */
template<class T>
class LocalRetriever : public PieceRetriever<T> {
public:
	/**
	 * Constructs a new instance for the given cache
	 * @param cache the underlying cache
	 */
	LocalRetriever( const NodeCache<T> &cache );
	virtual ~LocalRetriever() = default;

	/**
	 * Fetches the cache entry pointed to by the given semantic id and CacheRef
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item fetched
	 */
	virtual std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const;

	/**
	 * Computes the item described by the given operator tree and query rectangle
	 * @param op the operator tree
	 * @param query the query's bounds
	 * @param qp the profiler to use
	 * @return the computed result
	 */
	std::unique_ptr<T> compute( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) const;
protected:
	const NodeCache<T> &cache;
};

/**
 * Retriever implementation combining the local and global cache
 */
template<class T>
class RemoteRetriever : public LocalRetriever<T> {
public:
	/**
	 * Constructs a new instance for the given cache and handler
	 * @param cache the underlying cache
	 * @param handler the CacheRef handler
	 */
	RemoteRetriever( const NodeCache<T> &cache, const CacheRefHandler &handler );

	/**
	 * Fetches the cache entry pointed to by the given semantic id and CacheRef.
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item fetched
	 */
	std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const;

	/**
	 * Loads the desired item from a foreign node
	 * @param semantic_id the semantic id
	 * @param ref the reference describing the entry
	 * @param qp the profiler to use
	 * @return the item retrieved
	 */
	std::unique_ptr<T> load( const std::string &sematic_id, const CacheRef &ref, QueryProfiler &qp ) const;
private:
	/**
	 * Reads an data-item from the given buffer
	 * @param buffer the buffer to read the item from
	 */
	std::unique_ptr<T> read_item( BinaryReadBuffer &buffer ) const;
	const CacheRefHandler &ref_handler;
};

/////////////////////////////////////
//
// PUZZLER
//
/////////////////////////////////////


/**
 * Interface for puzzling a result from several parts
 */
template<class T>
class Puzzler {
public:
	virtual ~Puzzler() = default;
	/**
	 * Puzzles the given items into a result with the dimensions of bbox.
	 * @param bbox the bounding box of the result
	 * @param items the puzzle-pieces
	 * @return the combined result
	 */
	virtual std::unique_ptr<T> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>> &items) const = 0;
};

/**
 * Raster implementation of the Puzzler-interface
 */
class RasterPuzzler : public Puzzler<GenericRaster> {
public:
	std::unique_ptr<GenericRaster> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericRaster>> &items) const;
};

/**
 * Plot implementation of the Puzzler-interface
 */
class PlotPuzzler : public Puzzler<GenericPlot> {
public:
	std::unique_ptr<GenericPlot> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericPlot>> &items) const;
};

/**
 * Puzzler implementation for SimpleFeatureCollections
 */
template<class T>
class SimpleFeaturePuzzler : public Puzzler<T> {
public:
	virtual ~SimpleFeaturePuzzler() = default;
	std::unique_ptr<T> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>> &items) const;
protected:
	/**
	 * Helper for child-classes to append a vector of indices
	 * @param dest the target vector
	 * @param src the source vector
	 */
	void append_idx_vec( std::vector<uint32_t> &dest, const std::vector<uint32_t> &src ) const;

	/**
	 * To be implemented by child classes. Appends required indexes.
	 * May use append_idx_vec.
	 * @param dest the result collection
	 * @param src the source collection
	 */
	virtual void append_idxs( T &dest, const T &src ) const = 0;
};

/**
 * Point implementation of the SimpleFeatureCollection puzzler.
 */
class PointCollectionPuzzler : public SimpleFeaturePuzzler<PointCollection> {
protected:
	void append_idxs( PointCollection &dest, const PointCollection &src ) const;
};

/**
 * Line implementation of the SimpleFeatureCollection puzzler.
 */
class LineCollectionPuzzler : public SimpleFeaturePuzzler<LineCollection> {
protected:
	void append_idxs( LineCollection &dest, const LineCollection &src ) const;
};

/**
 * Polygon implementation of the SimpleFeatureCollection puzzler.
 */
class PolygonCollectionPuzzler : public SimpleFeaturePuzzler<PolygonCollection> {
protected:
	void append_idxs( PolygonCollection &dest, const PolygonCollection &src ) const;
};


/////////////////////////////////////
//
// PUZZLE UTIL
//
/////////////////////////////////////

/**
 * Utility to process puzzle-requests
 */
template<class T>
class PuzzleUtil {
public:
	/**
	 * Constructs a new instance
	 * @param retriever the retriever to use
	 * @param puzzler the puzzler instance
	 */
	PuzzleUtil( const PieceRetriever<T> &retriever, std::unique_ptr<Puzzler<T>> puzzler );

	/**
	 * Processes the given puzzle requests by fetching all parts, computing the remainders
	 * and combining the results.
	 * @param request the puzzle-request
	 * @param qp the profiler to use
	 * @return the result of the puzzle-request
	 */
	std::unique_ptr<T> process_puzzle( const PuzzleRequest &request, QueryProfiler &qp ) const;
private:
	/**
	 * Conmputes the remainder-queries
	 * @param semantic_id the semantic id
	 * @param ref_result a result used as reference for resolution computation
	 * @param request the puzzle-request
	 * @param qp the profiler to use
	 * @return the results of the remainder queries
	 */
	std::vector<std::unique_ptr<T>> compute_remainders( const std::string &semantic_id, const T& ref_result, const PuzzleRequest &request, QueryProfiler &profiler ) const;

	/**
	 * Enlarges the result of the puzzle-request to the maximum bounding cube.
	 * @param query the query-rectangle of the request
	 * @param items the puzzle-pieces
	 * @return the maximum bounding box of the result
	 */
	SpatioTemporalReference enlarge_puzzle( const QueryRectangle &query, const std::vector<std::shared_ptr<const T>>& items) const;
	const PieceRetriever<T> &retriever;
	std::unique_ptr<Puzzler<T>> puzzler;
};

#endif /* NODE_PUZZLE_UTIL_H_ */
