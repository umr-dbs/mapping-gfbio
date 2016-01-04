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
class NodePos;

//
// Describes the reorganization-tasks for a specific node
//
class NodeReorgDescription : public ReorgDescription {
public:
	NodeReorgDescription( std::shared_ptr<Node> node );
	const std::shared_ptr<Node> node;
};

//
// Tells the index cache if and how to reorganize
// its entries in order to get a balanced usage
// across all nodes
//
class ReorgStrategy {
public:
	static std::unique_ptr<ReorgStrategy> by_name( const IndexCache &cache, const std::string &name);
	virtual ~ReorgStrategy();
	virtual bool requires_reorg(const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const = 0;
	virtual void reorganize(std::map<uint32_t,NodeReorgDescription> &result ) const = 0 ;
	virtual uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const = 0;

	static bool entry_less(const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b);
	static bool entry_greater(const std::shared_ptr<IndexCacheEntry> &a, const std::shared_ptr<IndexCacheEntry> &b);
	static double get_score( const IndexCacheEntry &entry );
protected:
	ReorgStrategy(const IndexCache &cache, double target_usage);
	double get_target_usage( const std::map<uint32_t,NodeReorgDescription> &result ) const;
	const IndexCache &cache;
	const double max_target_usage;
};

//
// This strategy never triggers reorganization
//
class NeverReorgStrategy : public ReorgStrategy {
public:
	NeverReorgStrategy(const IndexCache &cache);
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result ) const;
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
};

//
// This strategy simply redistributes entries
// to achieve approx. the same memory usage across
// all nodes
//
class CapacityReorgStrategy : public ReorgStrategy {
public:
	CapacityReorgStrategy(const IndexCache &cache, double target_usage);
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result ) const;
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
};


//
// This strategy calculates the center of mass of over all entries
// and clusters nearby entries at a single node
//
class GeographicReorgStrategy : public ReorgStrategy {
	friend class NodePos;
public:
	GeographicReorgStrategy(const IndexCache &cache, double target_usage);
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result ) const;
	// It must be ensured that at least on node is present in the given map
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
private:
	std::map<uint32_t,NodePos> calculate_node_pos(const std::map<uint32_t,NodeReorgDescription> &result) const;
	uint32_t get_closest_node( const QueryCube &cube ) const;

	mutable std::map<uint32_t,NodePos> n_pos;
	static GDAL::CRSTransformer geosmsg_trans;
	static GDAL::CRSTransformer webmercator_trans;
};


//
// This strategy clusters cache-entries
// by similarity of their operator-graphs
//
class GraphReorgStrategy : public ReorgStrategy {
private:
	class GNode {
	public:
		typedef CacheStructure<std::pair<uint32_t,uint64_t>,IndexCacheEntry> Struct;

		GNode( const std::string &semantic_id, const Struct *structure );

		void append( std::shared_ptr<GNode> n );

		void mark();

		bool is_marked();

		const std::string semantic_id;
		const Struct *structure;
		std::vector<std::shared_ptr<GNode>> children;
	private:
		bool _mark;
	};

	class TempNode {
	public:
		TempNode( uint32_t id, uint64_t max_size );

		bool fits( const std::shared_ptr<GraphReorgStrategy::GNode> &gn, double target_usage ) const;
		void add( const std::shared_ptr<GraphReorgStrategy::GNode> &gn );
		std::shared_ptr<GraphReorgStrategy::GNode> remove_first();
		double get_usage() const;
		double get_usage_with(const std::shared_ptr<GraphReorgStrategy::GNode>& gn) const;
		double get_usage_without(const std::shared_ptr<GraphReorgStrategy::GNode>& gn) const;

		bool operator<( const TempNode &other ) const;

		uint32_t id;
		uint64_t max_size;
		uint64_t size;
		std::deque<std::shared_ptr<GraphReorgStrategy::GNode>> entries;

	};
public:
	static void append( std::shared_ptr<GNode> node, std::vector<std::shared_ptr<GNode>> &roots );

	GraphReorgStrategy(const IndexCache &cache, double target_usage);
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result ) const;
	uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;
private:
	std::vector<std::shared_ptr<GNode>> build_graph() const;
	std::vector<std::shared_ptr<GNode>> build_order( const std::vector<std::shared_ptr<GNode>> &roots ) const;
	void shift_in( const std::shared_ptr<GNode> &node, std::vector<TempNode> &nodes ) const;
	uint32_t find_node_for_graph( const GenericOperator &op ) const;
	mutable std::map<std::string,uint32_t> assignments;
};



#endif /* REORG_STRATEGY_H_ */
