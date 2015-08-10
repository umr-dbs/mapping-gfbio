/*
 * reorg_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache/index/reorg_strategy.h"
#include "cache/index/indexserver.h"
#include <ctime>
#include <algorithm>

NodeReorgDescription::NodeReorgDescription(uint32_t node_id) :
	node_id(node_id) {
}


//
// ReorgStrategy
//

ReorgStrategy::ReorgStrategy() {
}

ReorgStrategy::~ReorgStrategy() {
}

bool ReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	double maxru(0);
	double minru(1);

	// TODO: Think about this
	for (auto &e : nodes) {
		maxru = std::max(maxru, e.second->stats.get_raster_usage());
		minru = std::min(minru, e.second->stats.get_raster_usage());
	}
	return maxru - minru > 0.15;
}

//
// Never reorg
//

NeverReorgStrategy::NeverReorgStrategy() {
}

NeverReorgStrategy::~NeverReorgStrategy() {
}

bool NeverReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) nodes;
	return false;
}

std::vector<NodeReorgDescription> NeverReorgStrategy::reorganize(const IndexCache& raster_cache,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) {
	(void) raster_cache;
	(void) nodes;
	return std::vector<NodeReorgDescription>();
}



//
// Capacity based reorg
//

CapacityReorgStrategy::CapacityReorgStrategy() {
}

CapacityReorgStrategy::~CapacityReorgStrategy() {
}

std::vector<NodeReorgDescription> CapacityReorgStrategy::reorganize(
	const IndexCache &raster_cache,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) {


	std::unordered_map<uint32_t,std::vector<std::shared_ptr<IndexCacheEntry>>&> per_node;

	// Calculate mean usage
	double raster_accum(0);
	for (auto &e : nodes) {
		raster_accum += e.second->stats.get_raster_usage();

		auto &node_entries = raster_cache.get_node_entries(e.second->id);

		// Sort according to score
		std::sort(node_entries.begin(), node_entries.end(), entry_less);
		per_node.emplace( e.second->id, node_entries );
	}
	double target_mean = std::min( 0.8, raster_accum / nodes.size());

	// Find overflowing nodes
	std::vector<std::shared_ptr<IndexCacheEntry>> overflow;
	std::vector<std::shared_ptr<Node>> underflow_nodes;

	for (auto &e : nodes) {
		size_t target_bytes = e.second->stats.get_raster_cache_total() * target_mean;
		size_t bytes_used = e.second->stats.get_raster_cache_used();

		if ( bytes_used < target_bytes ) {
			underflow_nodes.push_back(e.second);
		}
		else  {
			auto &node_entries = per_node.at(e.first);
			auto iter = node_entries.begin();
			while ( iter != node_entries.end() && bytes_used > target_bytes ) {
				overflow.push_back(*iter);
				bytes_used -= (*iter)->size;
				iter++;
			}
		}
	}

	// Distribute overflow items
	std::vector<NodeReorgDescription> result;

	std::sort(overflow.begin(),overflow.end(), entry_greater);

	for (auto &node : underflow_nodes) {
		NodeReorgDescription desc(node->id);
		size_t target_bytes = node->stats.get_raster_cache_total() * target_mean;
		size_t bytes_used = node->stats.get_raster_cache_used();
		auto iter = overflow.begin();

		while ( iter != overflow.end() && bytes_used < target_bytes ) {

			if ( bytes_used + (*iter)->size <= target_bytes ) {
				auto &remote_node = nodes.at((*iter)->node_id);
				desc.add_item( ReorgItem( ReorgItem::Type::RASTER,
										  (*iter)->semantic_id,
										  remote_node->id,
										  (*iter)->entry_id,
										  remote_node->host,
										  remote_node->port) );
				iter = overflow.erase(iter);
			}
			else
				iter++;
		}
		if ( !desc.get_items().empty() )
			result.push_back(desc);
	}

	if ( !overflow.empty() ) {
		// TODO: Add removals
	}
	return result;
}

bool CapacityReorgStrategy::entry_less( const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b ) {
	return get_score(*a) < get_score(*b);
}

bool CapacityReorgStrategy::entry_greater(const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b) {
	return get_score(*a) > get_score(*b);
}

double CapacityReorgStrategy::get_score(const IndexCacheEntry& entry) {
	double hit_factor = 1.0 + std::min( entry.access_count / 1000.0, 1.0);
	return
	// Treat all the same within 10 seconds
	(entry.last_access / 10) * hit_factor;
}
