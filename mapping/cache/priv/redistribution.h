/*
 * redistribution.h
 *
 *  Created on: 12.07.2015
 *      Author: mika
 */

#ifndef REDISTRIBUTION_H_
#define REDISTRIBUTION_H_

#include "cache/priv/cache_structure.h"
#include "util/binarystream.h"
#include "cache/common.h"

#include <vector>

//
// Classes used to organize the redistribution of entries among the cache-nodes
//

//
// Notification about successful movement of an item
// for the index
//
class ReorgMoveResult : public TypedNodeCacheKey {
public:
	ReorgMoveResult( CacheType type, const std::string &semantic_id,
		uint64_t from_cache_id, uint32_t from_node_id, uint32_t to_node_id, uint64_t to_cache_id );

	ReorgMoveResult( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	uint32_t from_node_id;

	uint32_t to_node_id;

	uint64_t to_cache_id;
};

//
// Describes an item which should be moved from the given to_node
// to the executing node
//
class ReorgMoveItem : public TypedNodeCacheKey {
public:
	ReorgMoveItem( CacheType type, const std::string &semantic_id, uint64_t from_cache_id,
		uint32_t from_node_id, const std::string &from_host, uint32_t from_port );

	ReorgMoveItem( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;

	uint32_t from_node_id;
	// The host to retrieve the item from
	std::string from_host;
	// The port of the node to retrieve the item from
	uint32_t from_port;
};

//
// Bundles a set of move and remove operations
// for one reorganization
//
class ReorgDescription {
public:
	ReorgDescription();
	ReorgDescription( BinaryStream &stream );

	void add_move( ReorgMoveItem item );
	void add_removal( TypedNodeCacheKey item );
	const std::vector<ReorgMoveItem>& get_moves() const;
	const std::vector<TypedNodeCacheKey>& get_removals() const;

	bool is_empty() const;

	void toStream( BinaryStream &stream ) const;
private:
	std::vector<ReorgMoveItem> moves;
	std::vector<TypedNodeCacheKey> removals;

};

#endif /* REDISTRIBUTION_H_ */
