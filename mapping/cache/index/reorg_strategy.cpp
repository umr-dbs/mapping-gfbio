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
#include <deque>

NodeReorgDescription::NodeReorgDescription( std::shared_ptr<Node> node ) :
	node(node) {
}

//
// RELEVANCE
//

std::unique_ptr<RelevanceFunction> RelevanceFunction::by_name(
		const std::string& name) {
	if ( name == "LRU" )
		return make_unique<LRU>();
	else if ( name == "costLRU" )
		return make_unique<CostLRU>();
	throw ArgumentException(concat("Unknown Relevance-Function: ", name));
}

bool LRU::operator ()(const std::shared_ptr<const IndexCacheEntry>& e1, std::shared_ptr<const IndexCacheEntry>& e2) const {
	return e1->last_access > e2->last_access;
}

CostLRU::CostLRU() : now(0) {
}

void CostLRU::new_turn() {
	time(&now);
}

bool CostLRU::operator ()(const std::shared_ptr<const IndexCacheEntry>& e1,
		std::shared_ptr<const IndexCacheEntry>& e2) const {

	double f1 = 1.0 - (((now - e1->last_access) / 60) * 0.01);
	double f2 = 1.0 - (((now - e2->last_access) / 60) * 0.01);

	return (e1->costs * f1) > (e2->costs * f2);
}

//
// UTIL
//


bool ReorgNode::order_by_remaining_capacity_desc(const ReorgNode* n1,
		const ReorgNode* n2) {
	return n1->remaining_capacity() > n2->remaining_capacity();
}

ReorgNode::ReorgNode(uint32_t id, size_t target_size) :
	id(id), target_size(target_size), size(0) {
}

size_t ReorgNode::get_current_size() const {
	return size;
}

ssize_t ReorgNode::remaining_capacity() const {
	return (ssize_t) target_size - (ssize_t) size;
}

void ReorgNode::add(const std::shared_ptr<const IndexCacheEntry>& e) {
	size += e->size;
	entries.push_back( e );
}

bool ReorgNode::fits(const std::shared_ptr<const IndexCacheEntry>& e) const {
	return (size < target_size) && 2 * (target_size - size) >= e->size;
}

const std::vector<std::shared_ptr<const IndexCacheEntry> >& ReorgNode::get_entries() const {
	return entries;
}


//
// Strategy
//

std::unique_ptr<ReorgStrategy> ReorgStrategy::by_name(const IndexCache& cache,
		const std::string& name, const std::string& relevance) {

	double target_capacity = 0.8;
	std::unique_ptr<RelevanceFunction> rel = RelevanceFunction::by_name(relevance);
	if ( name == "capacity" )
		return make_unique<CapacityReorgStrategy>(cache,target_capacity,std::move(rel));
	else if ( name == "geo" )
		return make_unique<GeographicReorgStrategy>(cache,target_capacity,std::move(rel));
	else if ( name == "graph" )
		return make_unique<GraphReorgStrategy>(cache,target_capacity,std::move(rel));
	throw ArgumentException(concat("Unknown Reorg-Strategy: ", name));
}


ReorgStrategy::ReorgStrategy(const IndexCache& cache, double max_usage,
		std::unique_ptr<RelevanceFunction> relevance_function) :
	cache(cache), max_target_usage(max_usage), relevance_function(std::move(relevance_function)) {
}

ReorgStrategy::~ReorgStrategy() {
}

bool ReorgStrategy::requires_reorg(const std::map<uint32_t,std::shared_ptr<Node>>& nodes) const {
	double maxu = 0;
	double sum = 0;
	double sqsum = 0;


	for (auto &e : nodes) {
		double u = cache.get_capacity_usage(e.second->capacity);
		sum += u;
		sqsum += u*u;
		maxu = std::max(maxu, u);
	}

	double stddev = 0;
	if ( nodes.size() > 1 )
		stddev = std::sqrt( std::max( 0.0,
				// Incremental calculation of std-dev
				(sqsum - (sum*sum) / nodes.size()) / (nodes.size()) ));

	return  maxu >= 1.0 || stddev > 0.1;
}


uint32_t ReorgStrategy::get_least_used_node(
		const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {

	uint32_t min_id = 0;
	double min_usage = DoubleInfinity;

	for ( auto &kv : nodes ) {
		double usage = cache.get_capacity_usage(kv.second->capacity);
		if (  usage < min_usage ) {
			min_id = kv.first;
			min_usage = usage;
		}
	}
	if ( min_id == 0 )
		throw ArgumentException("No nodes given");
	return min_id;
}

void ReorgStrategy::reorganize(std::map<uint32_t,NodeReorgDescription> &result) {

	double bytes_used = 0;
	double bytes_available = 0;

	for ( auto &p : result ) {
		bytes_used      += cache.get_used_capacity( p.second.node->capacity );
		bytes_available += cache.get_total_capacity( p.second.node->capacity );
	}
	double target_cap = std::min( bytes_used / bytes_available, max_target_usage );
	auto all_entries = cache.get_all();

	// We have removals
	if ( bytes_used / bytes_available >= max_target_usage ) {
		relevance_function->new_turn();
		std::sort( all_entries.begin(), all_entries.end(), std::ref(*relevance_function) );

		while ( bytes_used / bytes_available >= max_target_usage ) {
			auto &e = all_entries.back();
			bytes_used -= e->size;
			result.at(e->node_id).add_removal( TypedNodeCacheKey(cache.get_reorg_type(),e->semantic_id,e->entry_id) );
			all_entries.pop_back();
		}
	}

	std::map<uint32_t, ReorgNode> distrib;
	for ( auto &p : result ) {
		size_t target_size = (target_cap * cache.get_total_capacity( p.second.node->capacity ));
		distrib.emplace( p.first, ReorgNode(p.first, target_size));
	}

	// Calculate distribution
	distribute( distrib, all_entries );

	// Create move-requests
	for ( auto &p : distrib ) {
		auto &tmp_res = result.at(p.first);
		for ( auto &e : p.second.entries ) {
			if ( e->node_id != p.first ) {
				auto &remote_node = result.at(e->node_id).node;
				tmp_res.add_move( ReorgMoveItem(
					cache.get_reorg_type(),
					e->semantic_id,
					e->entry_id,
					remote_node->id,
					remote_node->host,
					remote_node->port
				) );
			}
		}
	}
}


///////////////////////////////////////////////////////////////
//
// CAPACTIY BASED
//
///////////////////////////////////////////////////////////////


CapacityReorgStrategy::CapacityReorgStrategy(const IndexCache& cache,
		double target_usage,
		std::unique_ptr<RelevanceFunction> relevance_function) :
	ReorgStrategy(cache, target_usage, std::move(relevance_function)) {
}

bool CapacityReorgStrategy::node_sort(
		const std::shared_ptr<const IndexCacheEntry>& e1, std::shared_ptr<const IndexCacheEntry>& e2) {
	return e1->node_id < e2->node_id;
}

uint32_t CapacityReorgStrategy::get_node_for_job(const BaseRequest &request,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	(void) request;

	return get_least_used_node(nodes);
}

void CapacityReorgStrategy::distribute(std::map<uint32_t, ReorgNode>& result,
		std::vector<std::shared_ptr<const IndexCacheEntry> >& all_entries) {

	// Sort entries by node
	std::sort( all_entries.begin(), all_entries.end(), CapacityReorgStrategy::node_sort);

	uint32_t current_nid = 0;
	ReorgNode* current_node = nullptr;

	std::vector<std::shared_ptr<const IndexCacheEntry> > overflow;

	// Try to keep them on the same node
	while ( !all_entries.empty() ) {
		auto &e = all_entries.back();
		if ( e->node_id != current_nid ) {
			current_nid = e->node_id;
			current_node = &result.at(current_nid);
		}
		if ( current_node->fits(e) )
			current_node->add(e);
		else
			overflow.push_back(e);
		all_entries.pop_back();
	}

	if ( !overflow.empty() ) {
		std::vector<ReorgNode*> nodes;
		nodes.reserve(result.size());
		for ( auto &p : result)
			nodes.push_back( &p.second );
		distribute_overflow(overflow,nodes);
	}
}

void CapacityReorgStrategy::distribute_overflow(
		std::vector<std::shared_ptr<const IndexCacheEntry> >& entries,
		std::vector<ReorgNode*> underflow_nodes) {

	if ( underflow_nodes.size() == 1 ) {
		auto n = underflow_nodes.front();
		for ( auto &e : entries )
			n->add(e);
	}
	else {
		std::sort( underflow_nodes.begin(), underflow_nodes.end(), ReorgNode::order_by_remaining_capacity_desc );
		for ( auto &e : entries ) {
			underflow_nodes.front()->add(e);
			if ( underflow_nodes[0]->remaining_capacity() < underflow_nodes[1]->remaining_capacity() )
				std::sort( underflow_nodes.begin(), underflow_nodes.end(), ReorgNode::order_by_remaining_capacity_desc );
		}
	}
}

///////////////////////////////////////////////////////////////
//
// GRAPH BASED
//
///////////////////////////////////////////////////////////////

GraphReorgStrategy::GNode::GNode(const std::string& semantic_id) :
	semantic_id(semantic_id), _mark(false) {
}

void GraphReorgStrategy::GNode::append(std::shared_ptr<GNode> n) {
	GraphReorgStrategy::append( n, children );
}

void GraphReorgStrategy::GNode::mark() {
	_mark = true;
}

bool GraphReorgStrategy::GNode::is_marked() {
	return _mark;
}

void GraphReorgStrategy::GNode::add(std::shared_ptr<const IndexCacheEntry> &entry) {
	entries.push_back(entry);
}

//
// STRATEGY
//

void GraphReorgStrategy::append(std::shared_ptr<GNode> node,
		std::vector<std::shared_ptr<GNode>>& roots) {
	bool added = false;
	for ( size_t i = 0; i < roots.size(); i++ ) {
		if ( roots[i]->semantic_id.find(node->semantic_id) != std::string::npos ) {
			roots[i]->append(node);
			added = true;
		}
		else if ( node->semantic_id.find(roots[i]->semantic_id) != std::string::npos ) {
			node->append(roots[i]);
			roots[i] = node;
			added = true;
		}
	}
	if ( !added )
		roots.push_back(node);
}

GraphReorgStrategy::GraphReorgStrategy(const IndexCache& cache,
		double target_usage, std::unique_ptr<RelevanceFunction> relevance_function) :
	ReorgStrategy(cache,target_usage,std::move(relevance_function)) {
}

uint32_t GraphReorgStrategy::get_node_for_job(const BaseRequest& request,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {

	auto op = GenericOperator::fromJSON( request.semantic_id );
	uint32_t node = find_node_for_graph( *op );

	// Nothing found --> Pick node with max. free capacity
	if ( node == 0 )
		return get_least_used_node(nodes);
	else
		return node;
}

uint32_t GraphReorgStrategy::find_node_for_graph(const GenericOperator& op) const {
	std::deque<const GenericOperator*> queue;
	queue.push_back(&op);

	// Breadth first search
	while ( !queue.empty() ) {
		auto current = queue.front();
		auto it = assignments.find( current->getSemanticId() );
		// HIT
		if ( it != assignments.end() ) {
			return it->second;
		}
		// Add child-operators
		else {
			int src_cnt = 0;
			for ( int i = 0; i < GenericOperator::MAX_INPUT_TYPES; i++ )
				src_cnt += op.sourcecounts[i];

			for ( int i = 0; i < src_cnt; i++ ) {
				queue.push_back( op.sources[i] );
			}
		}
		queue.pop_front();
	}
	// No Node found...
	return 0;
}


void GraphReorgStrategy::distribute(std::map<uint32_t, ReorgNode>& result,
		std::vector<std::shared_ptr<const IndexCacheEntry> >& all_entries) {

	assignments.clear();
	auto ordered = build_order( build_graph(all_entries) );
	auto niter = result.begin();
	bool last_node = false;
	for ( auto &gn : ordered ) {
		for ( auto &entry : gn->entries ) {
			if ( !niter->second.fits(entry) && !last_node ) {
				niter++;
				if ( niter == result.end() ) {
					last_node = true;
					niter--;
				}
			}
			niter->second.add(entry);
		}
		assignments.emplace( gn->semantic_id, niter->first );
	}
}

std::vector<std::shared_ptr<GraphReorgStrategy::GNode>> GraphReorgStrategy::build_graph(std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries) {

	std::vector<std::shared_ptr<GNode>> roots;
	std::map<std::string, std::shared_ptr<GNode> > nodes;

	// Distribute entries to their workflow
	for ( auto &entry : all_entries )
		get_node(entry->semantic_id,nodes)->add(entry);

	// Build workflow-graph
	for ( auto &p : nodes )
		GraphReorgStrategy::append(p.second,roots);

	// Keep old ordering;
	std::map<std::string, std::shared_ptr<GNode>> root_map;
	for ( auto &r : roots )
		root_map.emplace( r->semantic_id, r );

	roots.clear();

	// Add all nodes still present from old order
	for ( auto &s : last_root_order ) {
		auto it = root_map.find(s);
		if ( it != root_map.end() ) {
			roots.push_back(it->second);
			root_map.erase(it);
		}
	}

	// Add remaining nodes
	for ( auto &p : root_map )
		roots.push_back(p.second);

	// Store order
	last_root_order.clear();
	for ( auto & r : roots )
		last_root_order.push_back(r->semantic_id);

	return roots;
}

std::shared_ptr<GraphReorgStrategy::GNode>& GraphReorgStrategy::get_node(const std::string& sem_id,
		std::map<std::string, std::shared_ptr<GNode> >& nodes) {

	auto it = nodes.find(sem_id);
	if ( it != nodes.end() )
		return it->second;
	else {
		std::shared_ptr<GNode> res( new GNode(sem_id) );
		return nodes.emplace( sem_id, res ).first->second;
	}
}

std::vector<std::shared_ptr<GraphReorgStrategy::GNode> > GraphReorgStrategy::build_order(
		const std::vector<std::shared_ptr<GNode> >& roots) {

	std::vector<std::shared_ptr<GNode>> result;

	// Root-wise breadth-first search
	for ( auto &root : roots ) {
		std::vector<std::shared_ptr<GNode>> v;
		v.push_back( root );
		for ( size_t i = 0; i < v.size(); i++ ) {
			for ( auto &c : v[i]->children ) {
				// Omit already added nodes
				if ( !c->is_marked() ) {
					c->mark();
					v.push_back(c);
				}
			}
		}
		result.insert( result.end(), v.begin(), v.end() );
	}
	return result;
}

///////////////////////////////////////////////////////////////
//
// GEOGRAPHIC
//
///////////////////////////////////////////////////////////////


const GDAL::CRSTransformer GeographicReorgStrategy::TRANS_GEOSMSG(EPSG_GEOSMSG,EPSG_LATLON);
const GDAL::CRSTransformer GeographicReorgStrategy::TRANS_WEBMERCATOR(EPSG_WEBMERCATOR,EPSG_LATLON);
const uint32_t GeographicReorgStrategy::MAX_Z = 0xFFFFFFFF;
const uint32_t GeographicReorgStrategy::MASKS[] = {0x55555555, 0x33333333, 0x0F0F0F0F, 0x00FF00FF};
const uint32_t GeographicReorgStrategy::SHIFTS[] = {1, 2, 4, 8};
const uint16_t GeographicReorgStrategy::SCALE_X = 0xFFFF / 360;
const uint16_t GeographicReorgStrategy::SCALE_Y = 0xFFFF / 180;

uint32_t GeographicReorgStrategy::get_z_value(const QueryCube& c) {
	Point<3> com = c.get_centre_of_mass();
	double ex = com.get_value(0);
	double ey = com.get_value(1);

	if ( c.epsg == EPSG_GEOSMSG ) {
		GeographicReorgStrategy::TRANS_GEOSMSG.transform(ex,ey);
	}
	else if ( c.epsg == EPSG_WEBMERCATOR ) {
		GeographicReorgStrategy::TRANS_WEBMERCATOR.transform(ex,ey);
	}

	// Translate and scale
	uint32_t x = (ex+180) * SCALE_X;
	uint32_t y = (ey+ 90) * SCALE_Y;

	x = (x | (x << SHIFTS[3])) & MASKS[3];
	x = (x | (x << SHIFTS[2])) & MASKS[2];
	x = (x | (x << SHIFTS[1])) & MASKS[1];
	x = (x | (x << SHIFTS[0])) & MASKS[0];

	y = (y | (y << SHIFTS[3])) & MASKS[3];
	y = (y | (y << SHIFTS[2])) & MASKS[2];
	y = (y | (y << SHIFTS[1])) & MASKS[1];
	y = (y | (y << SHIFTS[0])) & MASKS[0];

	uint32_t result = x | (y << 1);
	return result;


	return 0;
}

bool GeographicReorgStrategy::z_comp(
		const std::shared_ptr<const IndexCacheEntry>& e1,
		std::shared_ptr<const IndexCacheEntry>& e2) {

	return get_z_value( e1->bounds ) < get_z_value( e2->bounds );
}

GeographicReorgStrategy::GeographicReorgStrategy(const IndexCache& cache,
		double target_usage,
		std::unique_ptr<RelevanceFunction> relevance_function) :
	ReorgStrategy(cache,target_usage,std::move(relevance_function)) {
}

uint32_t GeographicReorgStrategy::get_node_for_job(const BaseRequest &request,
	const std::map<uint32_t, std::shared_ptr<Node> >& nodes) const {
	if ( !z_bounds.empty() ) {
		uint32_t z_value = get_z_value( QueryCube(request.query) );
		for ( auto &p : z_bounds )
			if ( z_value < p.first )
				return p.second;
		return z_bounds.back().second;
	}
	return get_least_used_node(nodes);
}

void GeographicReorgStrategy::distribute(std::map<uint32_t, ReorgNode>& result,
		std::vector<std::shared_ptr<const IndexCacheEntry> >& all_entries) {

	z_bounds.clear();

	std::sort(all_entries.begin(), all_entries.end(), GeographicReorgStrategy::z_comp);

	auto niter = result.begin();
	bool last_node = false;
	for ( size_t i = 0; i < all_entries.size(); i++ ) {
		if ( !niter->second.fits(all_entries[i]) && !last_node ) {
			uint32_t current_node_id = niter->first;
			niter++;
			if ( niter == result.end() ) {
				last_node = true;
				niter--;
			}
			else {
				uint32_t b = get_z_value( all_entries[i-1]->bounds );
				b =  b + (get_z_value( all_entries[i]->bounds) - b ) / 2;
				z_bounds.push_back( std::make_pair(b,current_node_id) );
			}
		}
		niter->second.add(all_entries[i]);
	}
	// Add last bound
	z_bounds.push_back( std::make_pair(MAX_Z,result.end()->first) );
}
