/*
 * reorg_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "reorg_strategy.h"

NodeReorgDescription::NodeReorgDescription(uint32_t node_id) : node_id(node_id) {
}

ReorgStrategy::ReorgStrategy() {
}

ReorgStrategy::~ReorgStrategy() {
}

bool ReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	// TODO
	return false;
}

CapacityReorgStrategy::CapacityReorgStrategy() {
}

CapacityReorgStrategy::~CapacityReorgStrategy() {
}

// TODO
std::vector<NodeReorgDescription> CapacityReorgStrategy::reorganize(
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes, const RasterRefCache& raster_cache) const {
	return std::vector<NodeReorgDescription>();
}
