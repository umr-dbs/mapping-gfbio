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
		maxru = std::max(maxru, e.second->capacity.get_raster_usage());
		minru = std::min(minru, e.second->capacity.get_raster_usage());
	}
	return maxru - minru > 0.15;
}

bool ReorgStrategy::entry_less( const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b ) {
	return get_score(*a) < get_score(*b);
}

bool ReorgStrategy::entry_greater(const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b) {
	return get_score(*a) > get_score(*b);
}

double ReorgStrategy::get_score(const IndexCacheEntry& entry) {
	double hit_factor = 1.0 + std::min( entry.access_count / 1000.0, 1.0);
	return
	// Treat all the same within 10 seconds
	(entry.last_access / 10) * hit_factor;
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
		raster_accum += e.second->capacity.get_raster_usage();

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
		size_t target_bytes = e.second->capacity.raster_cache_total * target_mean;
		size_t bytes_used = e.second->capacity.raster_cache_used;

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
		size_t target_bytes = node->capacity.raster_cache_total * target_mean;
		size_t bytes_used = node->capacity.raster_cache_used;
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

//
// Geographic reorg
//

class NodePos {
public:
	NodePos( uint32_t node_id, double x, double y );
	uint32_t node_id;
	double x;
	double y;
	std::vector<std::shared_ptr<IndexCacheEntry>> entries;
	double dist_to( const IndexCacheEntry& entry );
};

NodePos::NodePos(uint32_t node_id, double x, double y) :
	node_id(node_id), x(x), y(y) {
}

double NodePos::dist_to(const IndexCacheEntry& entry) {
	double ex = entry.bounds.x1 + (entry.bounds.x2-entry.bounds.x1)/2;
	double ey = entry.bounds.y1 + (entry.bounds.y2-entry.bounds.y1)/2;

	if ( entry.bounds.epsg == EPSG_GEOSMSG ) {
		GeographicReorgStrategy::geosmsg_trans.transform(ex,ey);
	}
	else if ( entry.bounds.epsg == EPSG_WEBMERCATOR ) {
		GeographicReorgStrategy::webmercator_trans.transform(ex,ey);
	}

	return sqrt( (ex-x)*(ex-x) + (ey-y)*(ey-y) );
}

GDAL::CRSTransformer GeographicReorgStrategy::geosmsg_trans(EPSG_GEOSMSG,EPSG_LATLON);
GDAL::CRSTransformer GeographicReorgStrategy::webmercator_trans(EPSG_WEBMERCATOR,EPSG_LATLON);

GeographicReorgStrategy::GeographicReorgStrategy() {
}

GeographicReorgStrategy::~GeographicReorgStrategy() {
}

std::vector<NodeReorgDescription> GeographicReorgStrategy::reorganize(const IndexCache& raster_cache,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) {
	// Calculate center of mass

	double weighted_x = 0, weighted_y = 0, mass = 0;

	for ( auto &kv : nodes ) {
		auto &node = *kv.second;

		for ( auto &e : raster_cache.get_node_entries(node.id) ) {
			auto &b = e->bounds;

			double x1 = b.x1;
			double x2 = b.x2;
			double y1 = b.y1;
			double y2 = b.y2;

			if ( b.epsg == EPSG_GEOSMSG ) {
				geosmsg_trans.transform(x1,y1);
				geosmsg_trans.transform(x2,y2);
			}
			else if ( b.epsg == EPSG_WEBMERCATOR ) {
				webmercator_trans.transform(x1,y1);
				webmercator_trans.transform(x2,y2);
			}
			else if ( b.epsg != EPSG_LATLON ) {
				Log::error("Unknown CRS: %d, ignoring in calculation.", b.epsg );
				continue;
			}

			double e_mass = (x2-x1)*(y2-y1);
			double e_x = x1 + (x2-x1)/2;
			double e_y = y1 + (y2-y1)/2;

			weighted_x += (e_x*e_mass);
			weighted_y += (e_y*e_mass);
			mass += e_mass;
		}
	}

	double cx = weighted_x / mass;
	double cy = weighted_y / mass;


	// Assign each node a point around center
	std::vector<NodePos> n_pos;

	double step = M_PI*2 / nodes.size();
	double angle = 0;

	Log::info("Center at: %f,%f, step: %f, nodes: %d", cx,cy, step, nodes.size());
	for ( auto & e : nodes ) {
		double x = std::cos(angle) + cx;
		double y = std::sin(angle) + cy;
		n_pos.push_back( NodePos( e.first, x, y) );
		Log::info("Node at: %f,%f", x,y);
		angle += step;
	}

	// Redistribute entries;
	for ( auto & kv : nodes ) {
		auto &node = *kv.second;
		for ( auto &e : raster_cache.get_node_entries(node.id) ) {
			size_t min_idx = 0;
			size_t current_idx = 0;
			double min_dist = DoubleInfinity;
			for ( auto &np : n_pos ) {
				double d = np.dist_to( *e);
				if ( d < min_dist ) {
					min_dist = d;
					min_idx = current_idx;
				}
				current_idx++;
			}
			n_pos.at(min_idx).entries.push_back(e);
		}
	}

	std::vector<std::shared_ptr<IndexCacheEntry>> overflow;
	std::vector<NodeReorgDescription> res;

	// Find overflowing nodes and create result
	for ( auto &np : n_pos ) {
		NodeReorgDescription desc(np.node_id);
		auto &node = *(nodes.at(np.node_id));
		size_t target = 0.8 * node.capacity.raster_cache_total;
		size_t used = 0;
		std::sort(np.entries.begin(), np.entries.end(), entry_greater);

		for ( auto & e : np.entries ) {
			if ( used + e->size < target ) {
				used += e->size;
				if ( e->node_id == np.node_id )
					continue;
				auto &remote_node = nodes.at(e->node_id);
				desc.add_item( ReorgItem( ReorgItem::Type::RASTER,
										  e->semantic_id,
										  remote_node->id,
										  e->entry_id,
										  remote_node->host,
										  remote_node->port ) );



			}
			else
				overflow.push_back( e );
		}
		if ( !desc.get_items().empty() )
			res.push_back(desc);
	}

	if ( !overflow.empty() ) {
		// TODO: Add removals
	}

	return res;
}
