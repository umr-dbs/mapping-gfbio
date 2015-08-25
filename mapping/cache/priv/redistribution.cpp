#include "cache/priv/redistribution.h"
#include "util/log.h"

ReorgRemoveItem::ReorgRemoveItem(Type type, const std::string& semantic_id, uint64_t cache_id) :
	NodeCacheKey(semantic_id, cache_id), type(type) {
}

ReorgRemoveItem::ReorgRemoveItem(BinaryStream& stream) :
	NodeCacheKey(stream) {
	stream.read(&type);
}

void ReorgRemoveItem::toStream(BinaryStream& stream) const {
	NodeCacheKey::toStream(stream);
	stream.write(type);
}

//
// ReorgResult
//

ReorgMoveResult::ReorgMoveResult(Type type, const std::string& semantic_id, uint64_t from_cache_id,
	uint32_t from_node_id, uint32_t to_node_id, uint64_t to_cache_id) :
	ReorgRemoveItem(type, semantic_id, from_cache_id), from_node_id(from_node_id), to_node_id(to_node_id), to_cache_id(
		to_cache_id) {
}

ReorgMoveResult::ReorgMoveResult(BinaryStream& stream) :
	ReorgRemoveItem(stream) {
	stream.read(&from_node_id);
	stream.read(&to_node_id);
	stream.read(&to_cache_id);
}

void ReorgMoveResult::toStream(BinaryStream& stream) const {
	ReorgRemoveItem::toStream(stream);
	stream.write(from_node_id);
	stream.write(to_node_id);
	stream.write(to_cache_id);
}

//
// Reorg Item
//

ReorgMoveItem::ReorgMoveItem(Type type, const std::string& semantic_id, uint64_t from_cache_id,
	uint32_t from_node_id, const std::string& from_host, uint32_t from_port) :
	ReorgRemoveItem(type, semantic_id, from_cache_id), from_node_id(from_node_id), from_host(from_host), from_port(
		from_port) {
}

ReorgMoveItem::ReorgMoveItem(BinaryStream& stream) :
	ReorgRemoveItem(stream) {
	stream.read(&from_node_id);
	stream.read(&from_host);
	stream.read(&from_port);
}

void ReorgMoveItem::toStream(BinaryStream& stream) const {
	ReorgRemoveItem::toStream(stream);
	stream.write(from_node_id);
	stream.write(from_host);
	stream.write(from_port);
}

//
// ReorgDescription
//

ReorgDescription::ReorgDescription() {
}

ReorgDescription::ReorgDescription(BinaryStream& stream) {
	uint64_t size;
	stream.read(&size);
	moves.reserve(size);
	for (uint64_t i = 0; i < size; i++)
		moves.push_back(ReorgMoveItem(stream));

	stream.read(&size);
	removals.reserve(size);
	for (uint64_t i = 0; i < size; i++)
		removals.push_back(ReorgRemoveItem(stream));

}

void ReorgDescription::add_move(ReorgMoveItem item) {
	moves.push_back(item);
}

const std::vector<ReorgMoveItem>& ReorgDescription::get_moves() const {
	return moves;
}

void ReorgDescription::add_removal(ReorgRemoveItem item) {
	removals.push_back(item);
}

const std::vector<ReorgRemoveItem>& ReorgDescription::get_removals() const {
	return removals;
}

bool ReorgDescription::is_empty() const {
	return moves.empty() && removals.empty();
}

void ReorgDescription::toStream(BinaryStream& stream) const {
	uint64_t size = moves.size();
	stream.write(size);
	for (auto &item : moves)
		item.toStream(stream);

	size = removals.size();
	stream.write(size);
	for (auto &item : removals)
		item.toStream(stream);
}

