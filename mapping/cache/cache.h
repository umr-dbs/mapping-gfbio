/*
 * local_cache.h
 *
 *  Created on: 11.05.2015
 *      Author: mika
 */

#ifndef CACHE_H_
#define CACHE_H_

#include "cache/priv/types.h"
#include "datatypes/spatiotemporal.h"
#include "datatypes/raster.h"
#include "util/log.h"

#include <geos/geom/Geometry.h>

#include <string>
#include <memory>
#include <vector>
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>

class SocketConnection;
class GenericOperator;

template<typename EType>
class STCacheStructure;


//
// Unique key generated for an entry in the cache
//
class STCacheKey {
public:
	STCacheKey( const std::string &semantic_id, uint64_t entry_id );
	STCacheKey( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	std::string semantic_id;
	uint64_t entry_id;
};

//
// Result of a cache-query.
// Holds two polygons:
// - The area covered by cache-entries
// - The remainder area
// and  list of ids referencing the entries
class STQueryResult {
public:
	typedef geos::geom::Geometry Geom;
	typedef std::unique_ptr<Geom> GeomP;

	// Constructs an empty result with the given query-rectangle as remainder
	STQueryResult( const QueryRectangle &query );
	STQueryResult( GeomP &covered, GeomP &remainder, double coverage, const std::vector<uint64_t> &ids );

	STQueryResult( const STQueryResult &r );
	STQueryResult( STQueryResult &&r );

	STQueryResult& operator=( const STQueryResult &r );
	STQueryResult& operator=( STQueryResult &&r );

	bool has_hit();
	bool has_remainder();
	std::string to_string();

	GeomP covered;
	GeomP remainder;
	double coverage;
	std::vector<uint64_t> ids;
};

//
// Interface describing the structure in which cache-entries
// are saved.
// A call to remove deletes the entry from the cache.
//
template<typename EType>
class STCacheStructure {
public:
	virtual ~STCacheStructure();
	// Inserts the given result into the cache. The cache copies the content of the result.
	virtual uint64_t insert( const std::unique_ptr<EType> &result ) = 0;

	// Fetches the entry by the given id and returns a copy
	virtual std::unique_ptr<EType> get_copy( const uint64_t id ) const = 0;
	// Fetches the entry by the given id. The result is read-only
	// and not copied.
	virtual const std::shared_ptr<EType> get( const uint64_t id ) const = 0;

	// Queries the cache with the given query-rectangle
	virtual const STQueryResult query( const QueryRectangle &spec ) const;

	// Returns the total size of the entry with the given id
	virtual uint64_t get_entry_size( const uint64_t id ) const = 0;
	// Removes the entry with the given id
	virtual void remove( const uint64_t id ) = 0;

protected:
	virtual std::unique_ptr<std::priority_queue<STQueryInfo>> get_query_candidates( const QueryRectangle &spec ) const = 0;

};

//
// Abstract cache class. To be completed per Entry-Type
//
template<typename EType>
class STCache {
public:
	STCache( size_t max_size );
	virtual ~STCache();
	// Adds an entry for the given semantic_id to the cache.
	virtual STCacheKey put( const std::string &semantic_id, const std::unique_ptr<EType> &item);
	// Removes the entry with the given key from the cache
	void remove( const STCacheKey &key );
	// Removes the entry with the given semantic_id and entry_id from the cache
	virtual void remove( const std::string &semantic_id, uint64_t entry_id );
	// Retrieves the entry with the given key. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const STCacheKey &key ) const;
	// Retrieves the entry with the given semantic_id and entry_id. A copy is returned which may be modified
	std::unique_ptr<EType> get_copy( const std::string &semantic_id, uint64_t id ) const;

	// Retrieves the entry with the given key. Cannot be modified.
	const std::shared_ptr<EType> get( const STCacheKey &key ) const;
	// Retrieves the entry with the given semantic_id and entry_id.  Cannot be modified.
	const std::shared_ptr<EType> get( const std::string &semantic_id, uint64_t id ) const;

	// Queries the cache with the given query-rectangle
	STQueryResult query( const std::string &semantic_id, const QueryRectangle &qr ) const;


	// Returns the maximum size (in bytes) this cache may hold
	size_t get_max_size() const { return max_size; }
	// Returns the current size (in bytes) of all entries stored in the cache
	size_t get_current_size() const { return current_size; }
protected:
	// Creates and returns a new structure for holding cache-entries
	// A single structure per semantic_id is used.
	virtual STCacheStructure<EType>* new_structure() const = 0;
private:
	// Retrieves the cache-structure for the given semantic_id.
	STCacheStructure<EType>* get_structure( const std::string &semantic_id, bool create = false) const;
	// Holds the maximum size (in bytes) this cache may hold
	size_t max_size;
	// Holds the current size (in bytes) of all entries stored in the cache
	size_t current_size;
	//std::unique_ptr<ReplacementPolicy<EType>> policy;
	// Holds all cache-structures accessable by the semantic_id
	mutable std::unordered_map<std::string,STCacheStructure<EType>*> caches;
	mutable std::mutex mtx;
};

//
// Raster Cache
//

class RasterCache : public STCache<GenericRaster> {
public:
	RasterCache() = delete;
	RasterCache( size_t size );
	virtual ~RasterCache();
protected:
	virtual STCacheStructure<GenericRaster>* new_structure() const;
};

//
// Index Raster Cache
//

class RasterRefCache : public STCache<STRasterRef> {
public:
	RasterRefCache();
	virtual ~RasterRefCache();
	void remove_all_by_node( uint32_t node_id );
	virtual void remove( const std::string &semantic_id, uint64_t id );
	virtual STCacheKey put( const std::string &semantic_id, const std::unique_ptr<STRasterRef> &item);
protected:
	virtual STCacheStructure<STRasterRef>* new_structure() const;
private:
	std::map<uint32_t,std::vector<STCacheKey>> entries_by_node;
};

#endif /* CACHE_H_ */
