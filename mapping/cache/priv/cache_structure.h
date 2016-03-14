/*
 * cache_structure.h
 *
 *  Created on: 09.08.2015
 *      Author: mika
 */

#ifndef CACHE_STRUCTURE_H_
#define CACHE_STRUCTURE_H_

#include "cache/priv/shared.h"
#include "cache/common.h"

#include <map>
#include <unordered_map>
#include <queue>
#include <memory>
#include <mutex>


/**
 * Holds information of how an cache-entry contributes
 * to the result of a query
 */
template<typename KType>
class CacheQueryInfo {
public:
	/**
	 * Constructs an instance with the given score, bounds and key
	 */
	CacheQueryInfo( const KType &key, const std::shared_ptr<const CacheEntry> &entry, double score );

	/**
	 * @return if the current instance is scored lower than the given one
	 */
	bool operator <(const CacheQueryInfo &b) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	KType key;

	std::shared_ptr<const CacheEntry> entry;

	double score;
};

/**
 * Represents the result of a cache-query. This contains:
 * <ul>
 * <li>A rectangle of the expected result (covered)</li>
 * <li>A list of keys representing the entries required to answer the query</li>
 * <li>A list of cubes representing the remaining calculations</li>
 * </ul>
 */
template<typename KType>
class CacheQueryResult {
public:

	/**
	 * Constructs an empty result with the given query-rectangle as remainder
	 * @param query the original query
	 */
	CacheQueryResult( const QueryRectangle &query );

	/**
	 * Constructs an result with the keys and remainders
	 * @param query the original query
	 * @param remainder the list of remainder queries
	 * @param keys the list of entry-keys required
	 */
	CacheQueryResult( QueryRectangle &&query, std::vector<Cube<3>> &&remainder, std::vector<KType> &&keys );

	/**
	 * @return whether the query has at least one hit in the cache
	 */
	bool has_hit() const;

	/**
	 * @return Whether the query has remainders
	 */
	bool has_remainder() const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	QueryRectangle covered;
	std::vector<KType> keys;
	std::vector<Cube<3>> remainder;
};

/**
 * This class models the d-dimensional cache-space
 */
template<typename KType, typename EType>
class CacheStructure {
public:
	/**
	 * Creates a new instance
	 */
	CacheStructure( const std::string &semantic_id );
	CacheStructure( const CacheStructure<KType,EType> & ) = delete;
	CacheStructure( CacheStructure<KType,EType> && ) = delete;

	CacheStructure<KType,EType>& operator=( const CacheStructure<KType,EType> & ) = delete;
	CacheStructure<KType,EType>& operator=( CacheStructure<KType,EType> && ) = delete;

	/**
	 * Inserts the given result into the cache, using the given key.
	 * @param key the key to use
	 * @param result the entry to insert
	 */
	void put( const KType &key, const std::shared_ptr<EType> &result );

	/**
	 * Removes the entry with the given key from the cache
	 * @param key the key of the entry to remove
	 * @return the removed entry
	 */
	std::shared_ptr<EType> remove( const KType &key );

	/**
	 * Retrieves the entry with the given key from the cache
	 * @param key the key of the entry to retrieve
	 * @return the entry for the given key
	 */
	std::shared_ptr<EType> get( const KType &key ) const;

	/**
	 * Queries the cache, using the given query-spec.
	 * @param spec the extend of the query
	 * @return the search result description
	 */
	const CacheQueryResult<KType> query( const QueryRectangle &spec ) const;

	/**
	 * @return a reference to all stored entries
	 */
	std::vector<std::shared_ptr<EType>> get_all() const;

	/**
	 * @return the size of all currently stored entries (in bytes)
	 */
	uint64_t size() const;

	/**
	 * @return the number of elements stored in the cache
	 */
	uint64_t num_elements() const;

private:
	/**
	 * @param key the key to stringify
	 * @return a human readable representation for the given key
	 */
	std::string key_to_string( const KType &key ) const;

	/**
	 * Searches the cache for candidates intersecting the given query-spec.
	 * @param spec the extend of the desired result
	 * @return a list of candidates ordered by their coverage
	 */
	std::priority_queue<CacheQueryInfo<KType>> get_query_candidates( const QueryRectangle &spec ) const;

	/**
	 * Tries to minimize the remainder-queries by unioning them. The uinion of two remainders
	 * is built, if the volumen of the new remainder does not exceed the volumes of the original remainders (by more than 1%)
	 * @param remainders the list of remainders
	 * @return the minimal list of remainder queries
	 */
	std::vector<Cube<3>> union_remainders( std::vector<Cube<3>> &remainders ) const;

	/**
	 * Calculates the size of the result, which may be larger than requested. This information may be used
	 * to combine client-queries more efficiently.
	 * @param orig the original query
	 * @param hits the cache-entries required to build the result
	 * @param remainders the remainders
	 * @return A query-rectangle describing the expected extend of the query-result
	 */
	QueryRectangle enlarge_expected_result( const QueryRectangle &orig, const std::vector<CacheQueryInfo<KType>> &hits, const std::vector<Cube<3>> &remainders ) const;

public:
	const std::string semantic_id;
private:
	std::map<KType, std::shared_ptr<EType>> entries;
	mutable RWLock lock;
	uint64_t _size;
};

/**
 * This class models a cache for a given result-type
 */
template<typename KType, typename EType>
class Cache {
public:
	Cache() = default;
	Cache( const Cache<KType,EType> & ) = delete;
	Cache( Cache<KType,EType> && ) = delete;
	Cache& operator=( const Cache<KType,EType> & ) = delete;
	Cache& operator=( Cache<KType,EType> && ) = delete;

	/**
	 * Queries the cache, using the given query-spec.
	 * @param semantic_id the semantic id of the query
	 * @param spec the extend of the query
	 * @return the search result description
	 */
	const CacheQueryResult<KType> query( const std::string &semantic_id, const QueryRectangle &qr ) const;
protected:
	/**
	 * Inserts an element into the cache-structure for the given semantic id
	 * @param semantic_id the semantic id
	 * @param entry the entry to insert
	 */
	void put_int( const std::string &semantic_id, const KType &key, const std::shared_ptr<EType> &entry );

	/**
	 * Retrieves all stored elements grouped by their semantic id.
	 * @return all elements stored in this cache
	 */
	std::unordered_map<std::string,std::vector<std::shared_ptr<EType>>> get_all_int() const;

	/**
	 * Retrieves the entry with the given key and semantic id
	 * @param semantic_id the semantic id
	 * @param key the entry's unique key
	 * @return the entry for the given key and semantic id
	 */
	std::shared_ptr<EType> get_int( const std::string &semantic_id, const KType &key ) const;

	/**
	 * Removes the entry with the given key and semantic id
	 * @param semantic_id the semantic id
	 * @param key the entry's unique key
	 * @return the removed entry
	 */
	std::shared_ptr<EType> remove_int( const std::string &semantic_id, const KType &key );
private:
	/**
	 * Helper to retrieve the cache-structure for a given semantic id
	 * @param semantic_id the semantic id to retrieve the structure for
	 * @param create tells whether a new structure should be created if none can be found
	 * @return the structure for the given semantic id
	 */
	CacheStructure<KType,EType>& get_cache( const std::string &semantic_id, bool create = false ) const;
	mutable std::unordered_map<std::string,std::unique_ptr<CacheStructure<KType,EType>>> caches;
	mutable std::mutex mtx;
};

#endif /* CACHE_STRUCTURE_H_ */
