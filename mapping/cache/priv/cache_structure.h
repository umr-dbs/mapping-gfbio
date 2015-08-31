/*
 * cache_structure.h
 *
 *  Created on: 09.08.2015
 *      Author: mika
 */

#ifndef CACHE_STRUCTURE_H_
#define CACHE_STRUCTURE_H_

#include "operators/queryrectangle.h"
#include "util/binarystream.h"

#include <geos/geom/Polygon.h>

#include <map>
#include <queue>
#include <memory>
#include <mutex>


//
// Describes the bounds of an raster-cache-entry.
// Additionally stores the resolution
//
class CacheEntryBounds : public SpatioTemporalReference {
public:
	CacheEntryBounds( SpatialReference sref, TemporalReference tref,
		QueryResolution::Type res_type = QueryResolution::Type::NONE,
		double x_res_from = 0, double x_res_to = 0, double y_res_from = 0, double y_res_to = 0 );
	CacheEntryBounds( const SpatioTemporalResult &result );
	CacheEntryBounds( const GridSpatioTemporalResult &result );
	CacheEntryBounds( BinaryStream &stream );

	// Checks if the bounds fully cover the given query-rectangle
	bool matches( const QueryRectangle &query ) const;
	// returns the spatial coverage if this entry for the given query-rectangle
	double get_coverage( const QueryRectangle &query ) const;

	// Serializes the bounds
	void toStream( BinaryStream &stream ) const;

	// returns a string representation
	std::string to_string() const;

	QueryResolution::Type res_type;

	double x_res_from, x_res_to, y_res_from, y_res_to;
};

//
// Basic information about cached data
//
class CacheEntry {
public:
	CacheEntry(CacheEntryBounds bounds, uint64_t size);
	CacheEntry( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	std::string to_string() const;

	CacheEntryBounds bounds;
	uint64_t size;
	time_t last_access;
	uint32_t access_count;
};

//
// Info about the coverage of a single entry in a cache-query
//
template<typename KType>
class CacheQueryInfo {
public:
	CacheQueryInfo( double coverage, double x1, double x2, double y1, double y2, KType key);
	bool operator <(const CacheQueryInfo &b) const;
	// the score of the entry
	double get_score() const;
	// a string representation
	std::string to_string() const;

	// coverage of issued query in [0,1]
	double coverage;
	// BBox of entry
	double x1, x2, y1, y2;

	KType key;
};

//
// Result of a cache-query.
// Holds two polygons:
// - The area covered by cache-entries
// - The remainder area
template<typename KType>
class CacheQueryResult {
public:
	typedef geos::geom::Geometry Geom;
	typedef std::unique_ptr<Geom> GeomP;

	// Constructs an empty result with the given query-rectangle as remainder
	CacheQueryResult( const QueryRectangle &query );
	CacheQueryResult( GeomP &covered, GeomP &remainder, double coverage, std::vector<KType> keys );

	CacheQueryResult( const CacheQueryResult &r );
	CacheQueryResult( CacheQueryResult &&r );

	CacheQueryResult& operator=( const CacheQueryResult &r );
	CacheQueryResult& operator=( CacheQueryResult &&r );

	bool has_hit() const;
	bool has_remainder() const;
	std::string to_string() const;

	GeomP covered;
	GeomP remainder;
	double coverage;

	std::vector<KType> keys;
};

//
// Unique key generated for an entry in the cache
//
class NodeCacheKey {
public:
	NodeCacheKey( const std::string &semantic_id, uint64_t entry_id );
	NodeCacheKey( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	std::string to_string() const;

	std::string semantic_id;
	uint64_t entry_id;
};

//
// Reference to a cache-entry on the node
//
class NodeCacheRef : public NodeCacheKey, public CacheEntry {
public:
	NodeCacheRef( const NodeCacheKey &key, const CacheEntry &entry );
	NodeCacheRef( const std::string semantic_id, uint64_t entry_id, const CacheEntry &entry );
	NodeCacheRef( BinaryStream &stream );

	void toStream(BinaryStream &stream) const;

	std::string to_string() const;
};

//
// The basic structure used for caching
//
template<typename KType, typename EType>
class CacheStructure {
public:
	// Inserts the given result into the cache. The cache copies the content of the result.
	void put( const KType &key, const std::shared_ptr<EType> &result );

	// Fetches the entry by the given id. The result is read-only
	// and not copied.
	std::shared_ptr<EType> get( const KType &key ) const;

	// Queries the cache with the given query-rectangle
	const CacheQueryResult<KType> query( const QueryRectangle &spec ) const;

	// Removes the entry with the given id
	std::shared_ptr<EType> remove( const KType &key );

	std::vector<std::shared_ptr<EType>> get_all() const;

private:
	std::map<KType, std::shared_ptr<EType>> entries;
	std::priority_queue<CacheQueryInfo<KType>> get_query_candidates( const QueryRectangle &spec ) const;
	mutable std::mutex mtx;
};

#endif /* CACHE_STRUCTURE_H_ */
