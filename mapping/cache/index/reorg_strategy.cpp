/*
 * reorg_strategy.cpp
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#include "cache/index/reorg_strategy.h"
#include "cache/index/indexserver.h"
#include "util/exceptions.h"
#include "util/concat.h"
#include <ctime>
#include <algorithm>

NodeReorgDescription::NodeReorgDescription( std::shared_ptr<Node> node ) :
	node(node) {
}

//
// ReorgStrategy
//

std::unique_ptr<ReorgStrategy> ReorgStrategy::by_name(const IndexCache &cache, const std::string &name) {
	if ( name == "none" )
		return make_unique<NeverReorgStrategy>(cache);
	else if ( name == "capacity" )
		return make_unique<CapacityReorgStrategy>(cache,0.8);
	else if ( name == "geo" )
		return make_unique<GeographicReorgStrategy>(cache,0.8);
	else if ( name == "graph" )
		return make_unique<GraphReorgStrategy>(cache,0.8);

	throw ArgumentException(concat("Unknown Reorg-Strategy: ", name));

}

ReorgStrategy::ReorgStrategy(const IndexCache &cache, double target_usage) : cache(cache), target_usage(target_usage) {
}

ReorgStrategy::~ReorgStrategy() {
}

double ReorgStrategy::get_target_usage( const std::map<uint32_t,NodeReorgDescription> &result ) const {
	// Calculate mean usage
	double accum(0);
	for (auto &e : result) {
		accum += cache.get_capacity_usage(e.second.node->capacity);
	}
	// Calculate target memory usage after reorg
	return std::min( 0.8, accum / result.size() * 1.05);
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

NeverReorgStrategy::NeverReorgStrategy(const IndexCache &cache) : ReorgStrategy(cache,0) {
}

NeverReorgStrategy::~NeverReorgStrategy() {
}

bool NeverReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) nodes;
	return false;
}

void NeverReorgStrategy::reorganize(std::map<uint32_t,NodeReorgDescription> &result) const {
	(void) result;
}

uint32_t NeverReorgStrategy::get_node_for_job(const QueryRectangle& query,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) query;

	int idx = std::rand() % nodes.size();
	auto iter = nodes.begin();
	for ( int i = 0; i < idx; i++ )
		iter++;
	return iter->first;
}


//
// Capacity based reorg
//

CapacityReorgStrategy::CapacityReorgStrategy(const IndexCache &cache, double target_usage) : ReorgStrategy(cache,target_usage)  {
}

CapacityReorgStrategy::~CapacityReorgStrategy() {
}

bool CapacityReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	double maxru(0);
	double minru(1);

	for (auto &e : nodes) {
		maxru = std::max(maxru, cache.get_capacity_usage(e.second->capacity));
		minru = std::min(minru, cache.get_capacity_usage(e.second->capacity));
	}
	return  maxru - minru > 0.15 || maxru >= 1.0;
}

void CapacityReorgStrategy::reorganize(std::map<uint32_t,NodeReorgDescription> &result) const {

	std::unordered_map<uint32_t,std::vector<std::shared_ptr<IndexCacheEntry>>&> per_node;
	double target_mean = get_target_usage( result );

	// Calculate mean usage
	for (auto &e : result) {
		auto &node_entries = cache.get_node_entries(e.second.node->id);

		// Sort according to score (ascending) -- for easy removal of least relevant entries
		std::sort(node_entries.begin(), node_entries.end(), entry_less);
		per_node.emplace( e.second.node->id, node_entries );
	}

	// Find overflowing nodes
	std::vector<std::shared_ptr<IndexCacheEntry>> overflow;
	std::vector<uint32_t> underflow_nodes;

	for (auto &e : result) {
		Log::debug("CapReorg: Capacity for node %d: %s", e.first, e.second.node->capacity.to_string().c_str());

		size_t target_bytes = cache.get_total_capacity(e.second.node->capacity) * target_mean;
		size_t bytes_used = cache.get_used_capacity(e.second.node->capacity);

		Log::debug("CapReorg: Target for node %d: %d/%d bytes", e.first, target_bytes, cache.get_total_capacity(e.second.node->capacity));

		if ( bytes_used < target_bytes ) {
			underflow_nodes.push_back(e.second.node->id);
		}
		else  {
			auto &node_entries = per_node.at(e.first);
			auto iter = node_entries.begin();
			while ( iter != node_entries.end() &&
				bytes_used > target_bytes ) {
				overflow.push_back(*iter);
				bytes_used -= (*iter)->size;
				iter++;
			}
		}

		Log::debug("CapReorg: Real usage after reorg for node %d: %d/%d bytes", e.first, bytes_used, cache.get_total_capacity(e.second.node->capacity) );
	}

	Log::debug("Items to redistribute: %d", overflow.size());

	// Redestribute overflow
	// Order by score descending to keep most relevant entries
	std::sort(overflow.begin(),overflow.end(), entry_greater);

	for (auto &node_id : underflow_nodes) {
		auto &desc = result.at(node_id);
		size_t target_bytes = cache.get_total_capacity(desc.node->capacity) * target_mean;
		size_t bytes_used = cache.get_used_capacity(desc.node->capacity);
		auto iter = overflow.begin();

		while ( iter != overflow.end() && bytes_used < target_bytes ) {

			if ( bytes_used + (*iter)->size <= target_bytes ) {
				auto &remote_node = result.at((*iter)->node_id).node;
				desc.add_move( ReorgMoveItem( cache.get_reorg_type(),
										  (*iter)->semantic_id,
										  (*iter)->entry_id,
										  remote_node->id,
										  remote_node->host,
										  remote_node->port) );
				iter = overflow.erase(iter);
			}
			else
				iter++;
		}
	}

	for ( auto &e : overflow ) {
		result.at(e->node_id).add_removal(
			TypedNodeCacheKey( cache.get_reorg_type(), e->semantic_id, e->entry_id )
		);
	}
}

uint32_t CapacityReorgStrategy::get_node_for_job(const QueryRectangle& query,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) query;

	uint32_t min_id = 0;
	double min_usage = DoubleInfinity;

	for ( auto &kv : nodes ) {
		double usage = cache.get_capacity_usage(kv.second->capacity);
		if (  usage < min_usage ) {
			min_id = kv.first;
			min_usage = usage;
		}
	}
	return min_id;
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
	double dist_to( epsg_t epsg, const Cube<3>& cube );
};

NodePos::NodePos(uint32_t node_id, double x, double y) :
	node_id(node_id), x(x), y(y) {
}

double NodePos::dist_to( epsg_t epsg, const Cube<3>& cube) {
	Point<3> com = cube.get_centre_of_mass();
	double ex = com.get_value(0);
	double ey = com.get_value(1);

	if ( epsg == EPSG_GEOSMSG ) {
		GeographicReorgStrategy::geosmsg_trans.transform(ex,ey);
	}
	else if ( epsg == EPSG_WEBMERCATOR ) {
		GeographicReorgStrategy::webmercator_trans.transform(ex,ey);
	}

	// Exact distance not required here
	return (ex-x)*(ex-x) + (ey-y)*(ey-y);
//	return sqrt( (ex-x)*(ex-x) + (ey-y)*(ey-y) );
}

GDAL::CRSTransformer GeographicReorgStrategy::geosmsg_trans(EPSG_GEOSMSG,EPSG_LATLON);
GDAL::CRSTransformer GeographicReorgStrategy::webmercator_trans(EPSG_WEBMERCATOR,EPSG_LATLON);

GeographicReorgStrategy::GeographicReorgStrategy(const IndexCache &cache, double target_usage) : ReorgStrategy(cache,target_usage) {
}

GeographicReorgStrategy::~GeographicReorgStrategy() {
}


//
// Reorganize if:
// - there is a new node
// - a node is gone
// - a node overflows
//
bool GeographicReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	double maxru(0);

	try {
		for (auto &e : nodes) {
			maxru = std::max(maxru, cache.get_capacity_usage(e.second->capacity));
			// Check if this node is present
			n_pos.at(e.first);
		}

		// Check gone nodes
		for ( auto &e : n_pos ) {
			nodes.at(e.first);
		}
	// We have a new or gone node;
	} catch ( const std::out_of_range &oor ) {
		return true;
	}
	return maxru >= 1;
}

void GeographicReorgStrategy::reorganize(std::map<uint32_t,NodeReorgDescription> &result) const {

	double target_mean = get_target_usage( result );

	n_pos = calculate_node_pos( result );

	if ( n_pos.empty() ) {
		Log::warn("Cannot reorganize without nodes");
		return;
	}

	// Redistribute entries;
	for ( auto &kv : result ) {
		for ( auto &e : cache.get_node_entries(kv.first) ) {
			uint32_t node_id = get_closest_node( e->bounds.epsg, e->bounds );
			n_pos.at(node_id).entries.push_back(e);
		}
	}

	std::vector<std::shared_ptr<IndexCacheEntry>> overflow;

	// Find overflowing nodes and create result
	for ( auto &kv : n_pos ) {
		auto &desc = result.at(kv.first);
		size_t target = target_mean * cache.get_total_capacity(desc.node->capacity);
		size_t used = 0;
		std::sort(kv.second.entries.begin(), kv.second.entries.end(), entry_greater);

		for ( auto & e : kv.second.entries ) {
			if ( used + e->size <= target ) {
				used += e->size;
				// Do not create move-item for items already at current node
				if ( e->node_id == kv.first )
					continue;
				auto &remote_node = result.at(e->node_id).node;
				desc.add_move( ReorgMoveItem( cache.get_reorg_type(),
										  e->semantic_id,
										  e->entry_id,
										  remote_node->id,
										  remote_node->host,
										  remote_node->port ) );
			}
			else
				overflow.push_back( e );
		}
	}

	for ( auto &e : overflow ) {
		result.at(e->node_id).add_removal(
			TypedNodeCacheKey( cache.get_reorg_type(), e->semantic_id, e->entry_id )
		);
	}
}

uint32_t GeographicReorgStrategy::get_node_for_job(const QueryRectangle& query,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {

	// Get closest node according to last reorg
	if ( !n_pos.empty() )
		return get_closest_node(query.epsg, QueryCube(query));
	// If no positions have been calculated yet
	else
		return nodes.begin()->first;
}

std::map<uint32_t,NodePos> GeographicReorgStrategy::calculate_node_pos(
	const std::map<uint32_t, NodeReorgDescription>& result) const {


	// Calculate center of mass
	double weighted_x = 0, weighted_y = 0, mass = 0;

	for ( auto &kv : result ) {
		auto &node = *kv.second.node;

		for ( auto &e : cache.get_node_entries(node.id) ) {
			auto &b = e->bounds;

			double x1 = b.get_dimension(0).a;
			double x2 = b.get_dimension(0).b;
			double y1 = b.get_dimension(1).a;
			double y2 = b.get_dimension(1).b;

			// Transform dimension if required
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

			double e_mass = e->size;
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
	std::map<uint32_t,NodePos> n_pos;

	double step = M_PI*2 / result.size();
	double angle = 0;

	Log::info("Center at: %f,%f, step: %f, nodes: %d", cx,cy, step, result.size());
	for ( auto & e : result ) {
		double x = std::cos(angle) + cx;
		double y = std::sin(angle) + cy;
		n_pos.emplace( e.first, NodePos( e.first, x, y) );
		Log::info("Node at: %f,%f", x,y);
		angle += step;
	}
	return n_pos;
}

uint32_t GeographicReorgStrategy::get_closest_node(  epsg_t epsg, const Cube<3> &cube ) const {
	double min_dist = DoubleInfinity;
	uint32_t min_id = 0;
	for ( auto &np : n_pos ) {
		double d = np.second.dist_to(epsg, cube);
		if ( d < min_dist ) {
			min_dist = d;
			min_id = np.first;
		}
	}
	return min_id;
}

//
//
//

GraphReorgStrategy::GraphReorgStrategy(const IndexCache &cache, double target_usage) : ReorgStrategy(cache,target_usage)  {
}

GraphReorgStrategy::~GraphReorgStrategy() {
}

void GraphReorgStrategy::reorganize(std::map<uint32_t,NodeReorgDescription> &result) const {
	(void) result;
	// TODO: Implement
}

bool GraphReorgStrategy::requires_reorg(const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) nodes;
	// TODO: Implement
	return false;
}

uint32_t GraphReorgStrategy::get_node_for_job(const QueryRectangle& query,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) query;
	// TODO: Implement
	return nodes.begin()->first;
}

