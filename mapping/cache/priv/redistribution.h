/*
 * redistribution.h
 *
 *  Created on: 12.07.2015
 *      Author: mika
 */

#ifndef REDISTRIBUTION_H_
#define REDISTRIBUTION_H_

#include "util/binarystream.h"

#include <vector>

class ReorgResult {
public:
	enum class Type : uint8_t { RASTER, POINT, LINE, POLYGON, PLOT };

	ReorgResult( Type type, const std::string &semantic_id,
		uint64_t cache_id, uint64_t idx_cache_id );

	ReorgResult( BinaryStream &stream );
	virtual ~ReorgResult();

	virtual void toStream( BinaryStream &stream ) const;

	Type type;
	// The semantic id of the entry
	std::string semantic_id;
	// The cache-id on the node of the entry
	uint64_t cache_id;
	// The cache-id on the index of the entry
	uint64_t idx_cache_id;

};

class ReorgItem : public ReorgResult {
public:
	ReorgItem( Type type, const std::string &host, uint32_t port, const std::string &semantic_id,
		uint64_t cache_id, uint64_t idx_cache_id );

	ReorgItem( BinaryStream &stream );
	virtual ~ReorgItem();

	virtual void toStream( BinaryStream &stream ) const;

	// The host to retrieve the item from
	std::string from_host;
	// The port of the node to retrieve the item from
	uint32_t from_port;
};

class ReorgDescription {
public:
	ReorgDescription();
	ReorgDescription( BinaryStream &stream );

	void add_item( ReorgItem item );
	void add_item( ReorgItem &&item );
	const std::vector<ReorgItem>& get_items() const;

	void toStream( BinaryStream &stream ) const;
private:
	std::vector<ReorgItem> items;

};


#endif /* REDISTRIBUTION_H_ */
