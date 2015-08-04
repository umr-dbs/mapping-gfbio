#include "cache/priv/redistribution.h"
#include "util/log.h"

ReorgResult::ReorgResult(Type type, const std::string& semantic_id, uint64_t cache_id,
	uint64_t idx_cache_id) : type(type), semantic_id(semantic_id),
	cache_id(cache_id), idx_cache_id(idx_cache_id) {
}

ReorgResult::ReorgResult(BinaryStream& stream) {
	stream.read(&type);
	stream.read(&semantic_id);
	stream.read(&cache_id);
	stream.read(&idx_cache_id);
}

ReorgResult::~ReorgResult() {
}

void ReorgResult::toStream(BinaryStream& stream) const {
	stream.write( type );
	stream.write( semantic_id );
	stream.write( cache_id );
	stream.write( idx_cache_id );
}


ReorgItem::ReorgItem(Type type, const std::string& host, uint32_t port, const std::string& semantic_id,
	uint64_t cache_id, uint64_t idx_cache_id) : ReorgResult(type, semantic_id, cache_id, idx_cache_id ),
	from_host(host), from_port(port) {
}

ReorgItem::ReorgItem(BinaryStream& stream) : ReorgResult(stream) {
	stream.read(&from_host);
	stream.read(&from_port);
}

ReorgItem::~ReorgItem() {
}

void ReorgItem::toStream(BinaryStream& stream) const {
	ReorgResult::toStream(stream);
	stream.write( from_host );
	stream.write( from_port );
}

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

void ReorgDescription::add_item(ReorgItem&& item) {
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
