/*
 * reorg_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef REORG_STRATEGY_H_
#define REORG_STRATEGY_H_

#include "cache/index/index_cache.h"
#include "cache/priv/redistribution.h"
#include "util/gdal.h"
#include <map>
#include <unordered_map>
#include <memory>
#include <set>

// Reorg-strategies manage the reconfiguration of the cache
// on imbalance

class Node;

//
// Describes the reorganization-tasks for a specific node
//
class NodeReorgDescription : public ReorgDescription {
public:
	NodeReorgDescription( std::shared_ptr<Node> node );
	NodeReorgDescription( const NodeReorgDescription & ) = delete;
	NodeReorgDescription( NodeReorgDescription && ) = default;
	NodeReorgDescription& operator=( const NodeReorgDescription & ) = delete;
	NodeReorgDescription& operator=( NodeReorgDescription && ) = default;

	const std::shared_ptr<Node> node;
};


class ReorgNode {
	friend class ReorgStrategy;
public:
	static bool order_by_remaining_capacity_desc( const ReorgNode *n1, const ReorgNode *n2 );

	ReorgNode( uint32_t id, size_t target_size );
	ReorgNode( const ReorgNode & ) = delete;
	ReorgNode( ReorgNode && ) = default;
	ReorgNode& operator=( const ReorgNode & ) = delete;
	ReorgNode& operator=( ReorgNode && ) = default;

	void add( const std::shared_ptr<const IndexCacheEntry> &e );
	bool fits( const std::shared_ptr<const IndexCacheEntry> &e ) const;
	const std::vector<std::shared_ptr<const IndexCacheEntry>>& get_entries() const;
	ssize_t remaining_capacity() const;
	size_t get_current_size() const;

	const uint32_t id;
	const size_t target_size;
private:
	size_t size;
	std::vector<std::shared_ptr<const IndexCacheEntry>> entries;
};


/**
 * Defines an ordering on cache-entries, so
 * that after sorting them, the least relevant entries
 * are at the end of the sorted structure
 */
class RelevanceFunction {
public:
	static std::unique_ptr<RelevanceFunction> by_name( const std::string &name );
	virtual ~RelevanceFunction() = default;
	virtual void new_turn() {};
	virtual bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
							  const std::shared_ptr<const IndexCacheEntry> &e2 ) const = 0;
};

class LRU : public RelevanceFunction {
public:
	bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
					  const std::shared_ptr<const IndexCacheEntry> &e2 ) const;
};

class CostLRU : public RelevanceFunction {
public:
	CostLRU();
	void new_turn();
	bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
					  const std::shared_ptr<const IndexCacheEntry> &e2 ) const;
private:
	time_t now;
};

//
// Tells the index cache if and how to reorganize
// its entries in order to get a balanced usage
// across all nodes
//
class ReorgStrategy {
public:
	static std::unique_ptr<ReorgStrategy> by_name( const IndexCache &cache, const std::string &name, const std::string &relevance = "LRU");

	ReorgStrategy( const IndexCache &cache, double max_usage, std::unique_ptr<RelevanceFunction> relevance_function);
	virtual ~ReorgStrategy();

	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	virtual uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const = 0;
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result );
protected:
	uint32_t get_least_used_node( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	virtual void distribute( std::map<uint32_t, ReorgNode> &result, std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries ) = 0;
private:
	bool nodes_changed( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	mutable std::set<uint32_t> last_nodes;
	const IndexCache &cache;
	const double max_target_usage;
	const std::unique_ptr<RelevanceFunction> relevance_function;
};

//
// This strategy simply redistributes entries
// to achieve approx. the same memory usage across
// all nodes
//
class CapacityReorgStrategy : public ReorgStrategy {
public:
	static bool node_sort( const std::shared_ptr<const IndexCacheEntry> &e1, const std::shared_ptr<const IndexCacheEntry> &e2 );
	CapacityReorgStrategy(const IndexCache &cache, double target_usage, std::unique_ptr<RelevanceFunction> relevance_function);
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
protected:
	void distribute( std::map<uint32_t, ReorgNode> &result, std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries );
private:
	void distribute_overflow( std::vector<std::shared_ptr<const IndexCacheEntry>> &entries, std::vector<ReorgNode*> underflow_nodes );
};


//
// This strategy clusters cache-entries
// by similarity of their operator-graphs
//
class GraphReorgStrategy : public ReorgStrategy {
private:
	class GNode {
	public:
		GNode( const std::string &semantic_id );
		void append( std::shared_ptr<GNode> n );
		void add( std::shared_ptr<const IndexCacheEntry> &entry );
		void mark();
		bool is_marked();
		const std::string semantic_id;
		std::vector<std::shared_ptr<const IndexCacheEntry>> entries;
		std::vector<std::shared_ptr<GNode>> children;
	private:
		bool _mark;
	};
public:
	static void append( std::shared_ptr<GNode> node, std::vector<std::shared_ptr<GNode>> &roots );
	GraphReorgStrategy(const IndexCache &cache, double target_usage, std::unique_ptr<RelevanceFunction> relevance_function);
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
protected:
	void distribute( std::map<uint32_t, ReorgNode> &result, std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries );
private:
	std::vector<std::shared_ptr<GNode>> build_graph( std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries );
	static std::shared_ptr<GNode>& get_node( const std::string &sem_id, std::map<std::string,std::shared_ptr<GNode>> &nodes );
	std::vector<std::shared_ptr<GNode>> build_order( const std::vector<std::shared_ptr<GNode>> &roots );
	uint32_t find_node_for_graph( const GenericOperator &op ) const;
	std::map<std::string,uint32_t> assignments;
	std::vector<std::string> last_root_order;
};

//
// This strategy calculates the center of mass of over all entries
// and clusters nearby entries at a single node
//
class GeographicReorgStrategy : public ReorgStrategy {
public:
	static uint32_t get_z_value( const QueryCube &c );
	static bool z_comp( const std::shared_ptr<const IndexCacheEntry> &e1, const std::shared_ptr<const IndexCacheEntry> &e2 );

	GeographicReorgStrategy(const IndexCache &cache, double target_usage, std::unique_ptr<RelevanceFunction> relevance_function);
	// It must be ensured that at least on node is present in the given map
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
protected:
	void distribute( std::map<uint32_t, ReorgNode> &result, std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries );
private:
	static const uint32_t MAX_Z;
	static const uint32_t MASKS[];
	static const uint32_t SHIFTS[];
	static const uint16_t SCALE_X;
	static const uint16_t SCALE_Y;
	static const GDAL::CRSTransformer TRANS_GEOSMSG;
	static const GDAL::CRSTransformer TRANS_WEBMERCATOR;

	std::vector<std::pair<uint32_t,uint32_t>> z_bounds;
};

#endif /* REORG_STRATEGY_H_ */
