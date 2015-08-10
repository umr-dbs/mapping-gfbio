#include "cache/priv/redistribution.h"
#include "util/log.h"

ReorgBase::ReorgBase(Type type, const std::string& semantic_id, uint32_t from_node_id,
	uint64_t from_cache_id) :
	type(type), semantic_id(semantic_id), from_cache_id(from_cache_id), from_node_id(from_node_id) {
}

ReorgBase::ReorgBase(BinaryStream& stream) {
	stream.read(&type);
	stream.read(&semantic_id);
	stream.read(&from_node_id);
	stream.read(&from_cache_id);
}

ReorgBase::~ReorgBase() {
}

void ReorgBase::toStream(BinaryStream& stream) const {
	stream.write(type);
	stream.write(semantic_id);
	stream.write(from_node_id);
	stream.write(from_cache_id);
}

//
// ReorgResult
//

ReorgResult::ReorgResult(Type type, const std::string& semantic_id, uint32_t from_node_id,
	uint64_t from_cache_id, uint32_t to_node_id, uint64_t to_cache_id) :
	ReorgBase(type, semantic_id, from_node_id, from_cache_id),
	to_node_id(to_node_id), to_cache_id(to_cache_id){
}

ReorgResult::ReorgResult(BinaryStream& stream) : ReorgBase(stream) {
	stream.read(&to_node_id);
	stream.read(&to_cache_id);
}

ReorgResult::~ReorgResult() {
}

void ReorgResult::toStream(BinaryStream& stream) const {
	ReorgBase::toStream(stream);
	stream.write( to_node_id );
	stream.write( to_cache_id );
}

//
// Reorg Item
//

ReorgItem::ReorgItem(Type type, const std::string& semantic_id, uint32_t from_node_id, uint64_t from_cache_id,
	const std::string& from_host, uint32_t from_port) :
	ReorgBase(type,semantic_id,from_node_id,from_cache_id), from_host(from_host), from_port(from_port) {
}

ReorgItem::ReorgItem(BinaryStream& stream) : ReorgBase(stream) {
	stream.read(&from_host);
	stream.read(&from_port);
}

ReorgItem::~ReorgItem() {
}

void ReorgItem::toStream(BinaryStream& stream) const {
	ReorgBase::toStream(stream);
	stream.write( from_host );
	stream.write( from_port );
}

//
// ReorgDescription
//

ReorgDescription::ReorgDescription() {
}

ReorgDescription::ReorgDescription(BinaryStream& stream) {
	uint64_t size;
	stream.read( &size );
	Log::debug("Reading %d items from stream", size);
	items.reserve(size);
	for ( uint64_t i = 0; i < size; i++ )
		items.push_back( ReorgItem(stream) );
}

void ReorgDescription::add_item(ReorgItem item) {
	items.push_back(item);
}

const std::vector<ReorgItem>& ReorgDescription::get_items() const {
	return items;
}

void ReorgDescription::toStream(BinaryStream& stream) const {
	uint64_t size = items.size();
	stream.write( size );
	for ( auto &item : items )
		item.toStream( stream );
}
