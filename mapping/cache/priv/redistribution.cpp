#include "cache/priv/redistribution.h"
#include "util/log.h"

///////////////////////////////////////////////////////////
//
// ReorgMoveResult
//
///////////////////////////////////////////////////////////

ReorgMoveResult::ReorgMoveResult(CacheType type, const std::string& semantic_id, uint32_t from_node_id,
	uint64_t from_cache_id, uint32_t to_node_id, uint64_t to_cache_id) :
	TypedNodeCacheKey(type, semantic_id, from_cache_id), from_node_id(from_node_id), to_node_id(to_node_id), to_cache_id(
		to_cache_id) {
}

ReorgMoveResult::ReorgMoveResult(BinaryReadBuffer& buffer) :
	TypedNodeCacheKey(buffer), from_node_id(buffer.read<uint32_t>() ),
	to_node_id(buffer.read<uint32_t>() ), to_cache_id(buffer.read<uint64_t>() ) {
}

void ReorgMoveResult::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	TypedNodeCacheKey::serialize(buffer, is_persistent_memory);
	buffer.write(from_node_id);
	buffer.write(to_node_id);
	buffer.write(to_cache_id);
}

///////////////////////////////////////////////////////////
//
// ReorgMoveItem
//
///////////////////////////////////////////////////////////

ReorgMoveItem::ReorgMoveItem(CacheType type, const std::string& semantic_id, uint32_t from_node_id,
	uint64_t from_cache_id, const std::string& from_host, uint32_t from_port) :
	TypedNodeCacheKey(type, semantic_id, from_cache_id), from_node_id(from_node_id), from_host(from_host), from_port(
		from_port) {
}

ReorgMoveItem::ReorgMoveItem(BinaryReadBuffer& buffer) :
	TypedNodeCacheKey(buffer), from_node_id(buffer.read<uint32_t>()),
	from_host(buffer.read<std::string>()), from_port(buffer.read<uint32_t>()) {
}

void ReorgMoveItem::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	TypedNodeCacheKey::serialize(buffer, is_persistent_memory);
	buffer.write(from_node_id);
	buffer.write(from_host);
	buffer.write(from_port);
}

///////////////////////////////////////////////////////////
//
// ReorgMoveDescription
//
///////////////////////////////////////////////////////////

ReorgDescription::ReorgDescription() {
}

ReorgDescription::ReorgDescription(BinaryReadBuffer& buffer) {
	uint64_t size = buffer.read<uint64_t>();
	moves.reserve(size);
	for (uint64_t i = 0; i < size; i++)
		moves.push_back(ReorgMoveItem(buffer));

	buffer.read(&size);
	removals.reserve(size);
	for (uint64_t i = 0; i < size; i++)
		removals.push_back(TypedNodeCacheKey(buffer));

}

void ReorgDescription::add_move(ReorgMoveItem item) {
	moves.push_back(item);
}

const std::vector<ReorgMoveItem>& ReorgDescription::get_moves() const {
	return moves;
}

void ReorgDescription::add_removal(TypedNodeCacheKey item) {
	removals.push_back(item);
}

const std::vector<TypedNodeCacheKey>& ReorgDescription::get_removals() const {
	return removals;
}

bool ReorgDescription::is_empty() const {
	return moves.empty() && removals.empty();
}

void ReorgDescription::serialize(BinaryWriteBuffer& buffer, bool is_persistent_memory) const {
	buffer.write(static_cast<uint64_t>(moves.size()));
	for (auto &item : moves)
		item.serialize(buffer,is_persistent_memory);

	buffer.write(static_cast<uint64_t>(removals.size()));
	for (auto &item : removals)
		item.serialize(buffer,is_persistent_memory);
}
