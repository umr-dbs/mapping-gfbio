/*
 * puzzle_util.h
 *
 *  Created on: 08.01.2016
 *      Author: mika
 */

#ifndef NODE_PUZZLE_UTIL_H_
#define NODE_PUZZLE_UTIL_H_

#include "cache/node/node_cache.h"
#include "cache/priv/transfer.h"
#include "operators/operator.h"

/////////////////////////////////////
//
// RETRIEVER
//
/////////////////////////////////////

class CacheRefHandler {
public:
	virtual ~CacheRefHandler() = default;
	virtual CacheRef create_self_ref(uint64_t id) const = 0;
	virtual bool is_self_ref(const CacheRef& ref) const = 0;
};

template<class T>
class PieceRetriever {
public:
	virtual ~PieceRetriever() = default;
	virtual std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const = 0;
	virtual std::unique_ptr<T> compute( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) const = 0;
};

template<class T>
class LocalRetriever : public PieceRetriever<T> {
public:
	LocalRetriever( const NodeCache<T> &cache );
	virtual ~LocalRetriever() = default;
	virtual std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const;
	std::unique_ptr<T> compute( GenericOperator &op, const QueryRectangle &query, QueryProfiler &qp ) const;
protected:
	const NodeCache<T> &cache;
};

template<class T>
class RemoteRetriever : public LocalRetriever<T> {
public:
	RemoteRetriever( const NodeCache<T> &cache, const CacheRefHandler &handler );
	std::shared_ptr<const T> fetch( const std::string &semantic_id, const CacheRef &ref, QueryProfiler &qp ) const;
	std::unique_ptr<T> load( const std::string &sematic_id, const CacheRef &ref, QueryProfiler &qp ) const;
private:
	std::unique_ptr<T> read_item( BinaryStream &stream ) const;
	const CacheRefHandler &ref_handler;
};

/////////////////////////////////////
//
// PUZZLER
//
/////////////////////////////////////

template<class T>
class Puzzler {
public:
	virtual ~Puzzler() = default;
	virtual std::unique_ptr<T> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>> &items) const = 0;
};

class RasterPuzzler : public Puzzler<GenericRaster> {
public:
	std::unique_ptr<GenericRaster> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericRaster>> &items) const;
};

class PlotPuzzler : public Puzzler<GenericPlot> {
public:
	std::unique_ptr<GenericPlot> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const GenericPlot>> &items) const;
};

template<class T>
class SimpleFeaturePuzzler : public Puzzler<T> {
public:
	virtual ~SimpleFeaturePuzzler() = default;
	std::unique_ptr<T> puzzle( const SpatioTemporalReference &bbox, const std::vector<std::shared_ptr<const T>> &items) const;
protected:
	void append_idx_vec( std::vector<uint32_t> &dest, const std::vector<uint32_t> &src ) const;
	virtual void append_idxs( T &dest, const T &src ) const = 0;
};

class PointCollectionPuzzler : public SimpleFeaturePuzzler<PointCollection> {
protected:
	void append_idxs( PointCollection &dest, const PointCollection &src ) const;
};

class LineCollectionPuzzler : public SimpleFeaturePuzzler<LineCollection> {
protected:
	void append_idxs( LineCollection &dest, const LineCollection &src ) const;
};

class PolygonCollectionPuzzler : public SimpleFeaturePuzzler<PolygonCollection> {
protected:
	void append_idxs( PolygonCollection &dest, const PolygonCollection &src ) const;
};


/////////////////////////////////////
//
// PUZZLE UTIL
//
/////////////////////////////////////

template<class T>
class PuzzleUtil {
public:
	PuzzleUtil( const PieceRetriever<T> &retriever, std::unique_ptr<Puzzler<T>> puzzler );
	std::unique_ptr<T> process_puzzle( const PuzzleRequest &request, QueryProfiler &qp ) const;
private:
	std::vector<std::unique_ptr<T>> compute_remainders( const std::string &semantic_id, const T& ref_result, const PuzzleRequest &request, QueryProfiler &profiler ) const;
	SpatioTemporalReference enlarge_puzzle( const QueryRectangle &query, const std::vector<std::shared_ptr<const T>>& items) const;
	const PieceRetriever<T> &retriever;
	std::unique_ptr<Puzzler<T>> puzzler;
};

#endif /* NODE_PUZZLE_UTIL_H_ */
