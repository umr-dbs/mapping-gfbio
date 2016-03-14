/*
 * redistribution.h
 *
 *  Created on: 12.07.2015
 *      Author: mika
 */

#ifndef REDISTRIBUTION_H_
#define REDISTRIBUTION_H_

#include "cache/priv/shared.h"
#include "util/binarystream.h"

#include <vector>

/**
 * Holds information about a successfully moved cache-entry
 */
class ReorgMoveResult : public TypedNodeCacheKey {
public:
	/**
	 * Constructs  new instance
	 * @param type the type of the cache-entry
	 * @param semantic_id the semantic-id
	 * @param from_node_id the id of the node the entry was fetched from
	 * @param from_cache_id the id of the entry at the source node
	 * @param to_node_id the id of the node the entry was moved to
	 * @param to_cache_id the id of the entry at the destination node
	 */
	ReorgMoveResult( CacheType type, const std::string &semantic_id,
		uint32_t from_node_id, uint64_t from_cache_id, uint32_t to_node_id, uint64_t to_cache_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	ReorgMoveResult( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize( BinaryWriteBuffer &buffer ) const;

	uint32_t from_node_id;

	uint32_t to_node_id;

	uint64_t to_cache_id;
};

/**
 * Describes a move-operation
 */
class ReorgMoveItem : public TypedNodeCacheKey {
public:
	/**
	 * Constructs  new instance
	 * @param type the type of the cache-entry
	 * @param semantic_id the semantic-id
	 * @param from_node_id the id of the node the entry was fetched from
	 * @param from_cache_id the id of the entry at the source node
	 * @param from_host the hostname of the source node
	 * @param from_port the port of the source node's delivery component
	 */
	ReorgMoveItem( CacheType type, const std::string &semantic_id, uint32_t from_node_id,
		uint64_t from_cache_id, const std::string &from_host, uint32_t from_port );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	ReorgMoveItem( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize( BinaryWriteBuffer &buffer ) const;

	uint32_t from_node_id;
	// The host to retrieve the item from
	std::string from_host;
	// The port of the node to retrieve the item from
	uint32_t from_port;
};

/**
 * Holds all actions to be taken in a reorg-cycle.
 * This includes the entries to be fetched from foreign nodes
 * as well as the entries to be removed from the local cache.
 */
class ReorgDescription {
public:
	/**
	 * Creates a new instance
	 */
	ReorgDescription();

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	ReorgDescription( BinaryReadBuffer &buffer );

	/**
	 * Adds a move-operation
	 * @param item the item to be moved
	 */
	void add_move( ReorgMoveItem item );

	/**
	 * Adds a remove-operation
	 * @param item the item to be removed
	 */
	void add_removal( TypedNodeCacheKey item );

	/**
	 * @return all items to be migrated from a foreign node
	 */
	const std::vector<ReorgMoveItem>& get_moves() const;

	/**
	 * @return all items to be removed from the local cache
	 */
	const std::vector<TypedNodeCacheKey>& get_removals() const;

	/**
	 * @return whether this description is empty (contains no operations).
	 */
	bool is_empty() const;

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize( BinaryWriteBuffer &buffer ) const;
private:
	std::vector<ReorgMoveItem> moves;
	std::vector<TypedNodeCacheKey> removals;

};

#endif /* REDISTRIBUTION_H_ */
