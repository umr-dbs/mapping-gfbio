/*
 * reorg_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef REORG_STRATEGY_H_
#define REORG_STRATEGY_H_

#include "cache/cache.h"
#include "cache/priv/redistribution.h"
#include <map>
#include <memory>


class Node;

class NodeReorgDescription : public ReorgDescription {
public:
	NodeReorgDescription( uint32_t node_id );
	const uint32_t node_id;
};

class ReorgStrategy {
public:
	ReorgStrategy();
	virtual ~ReorgStrategy();
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	virtual std::vector<NodeReorgDescription> reorganize( const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const RasterRefCache &raster_cache ) const = 0;
};

class CapacityReorgStrategy : public ReorgStrategy {
public:
	CapacityReorgStrategy();
	virtual ~CapacityReorgStrategy();
	virtual std::vector<NodeReorgDescription> reorganize( const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const RasterRefCache &raster_cache ) const;
};

#endif /* REORG_STRATEGY_H_ */
