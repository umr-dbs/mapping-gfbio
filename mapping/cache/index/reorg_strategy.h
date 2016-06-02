/*
 * reorg_strategy.h
 *
 *  Created on: 03.08.2015
 *      Author: mika
 */

#ifndef REORG_STRATEGY_H_
#define REORG_STRATEGY_H_

#include "cache/index/index_cache.h"
#include "cache/index/node.h"
#include "cache/priv/shared.h"
#include "cache/priv/redistribution.h"

#include "operators/operator.h"

#include "util/gdal.h"
#include <map>
#include <unordered_map>
#include <memory>
#include <set>

// Reorg-strategies manage the reconfiguration of the cache
// on imbalance

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

/**
 * Class holding all information about a node required during reorg.
 */
class ReorgNode {
	friend class ReorgStrategy;
public:
	/**
	 * Comparator function for ordering nodes by their remaining capacity
	 * @param n1 the first node
	 * @param n2 the second node
	 * @return whether the remaining capacity of e1 is greater than of n2
	 */
	static bool order_by_remaining_capacity_desc( const ReorgNode *n1, const ReorgNode *n2 );

	/**
	 * Creates a new instance
	 * @param id the node's id
	 * @param target_size the bytes which should be used after reorg
	 */
	ReorgNode( uint32_t id, size_t target_size );
	ReorgNode( const ReorgNode & ) = delete;
	ReorgNode( ReorgNode && ) = default;
	ReorgNode& operator=( const ReorgNode & ) = delete;
	ReorgNode& operator=( ReorgNode && ) = default;

	/**
	 * Adds an entry to this node (tells that it should be there after reorg).
	 * @param e the entry to add
	 */
	void add( const std::shared_ptr<const IndexCacheEntry> &e );

	/**
	 * @param e the entry to check
	 * @return whether the given entry fits onto this node
	 */
	bool fits( const std::shared_ptr<const IndexCacheEntry> &e ) const;

	/**
	 * @return all entries to be located at this node
	 */
	const std::vector<std::shared_ptr<const IndexCacheEntry>>& get_entries() const;

	/**
	 * @return the remainig capacity of this node
	 */
	ssize_t remaining_capacity() const;

	/**
	 * @return the currently used capacity of this node in bytes
	 */
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
	/**
	 * Compares the given entries by their relevance. If e1 is more relevant
	 * than e2, true is returned.
	 * @param e1 the first entry
	 * @param e2 the second entry
	 * @return whether e1 is more relevant than e2
	 */
	virtual bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
							  const std::shared_ptr<const IndexCacheEntry> &e2 ) const = 0;
};

/**
 * Simple LRU-Implementation of the relevance function
 */
class LRU : public RelevanceFunction {
public:
	bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
					  const std::shared_ptr<const IndexCacheEntry> &e2 ) const;
};

/**
 * A cost based LRU implementation of the relevance function.
 * Main factor for the ordering are the computation costs. They are
 * weighted with a time since the last access to the entry.
 */
class CostLRU : public RelevanceFunction {
public:
	CostLRU();
	void new_turn();
	bool operator() ( const std::shared_ptr<const IndexCacheEntry> &e1,
					  const std::shared_ptr<const IndexCacheEntry> &e2 ) const;
private:
	time_t now;
};

/**
 * Tells the index cache if and how to reorganize
 * its entries in order to get a balanced usage
 * across all nodes
 */
class ReorgStrategy {
public:
	/**
	 * Creates an instance for the given cache. The strategy is chosen from the name.
	 * @param cache the cache to reorganize
	 * @param name the name of the strategy
	 * @param relevance the name of the relevance function
	 */
	static std::unique_ptr<ReorgStrategy> by_name( const IndexCache &cache, const std::string &name, const std::string &relevance = "LRU");

	/**
	 * Constructs an instance for the given cache.
	 * @param cache the cache to reorganize
	 * @param relevance_function the relevance function to use
	 */
	ReorgStrategy( const IndexCache &cache, double max_usage, std::unique_ptr<RelevanceFunction> relevance_function);
	virtual ~ReorgStrategy() = default;

	/**
	 * @param nodes the currently active nodes
	 * @return whether a reorg for the managed cache is required or not
	 */
	bool requires_reorg( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;

	/**
	 * Finds the best node to schedule the given request on.
	 * @param request the request to schedule
	 * @param nodes the currently active nodes
	 * @return the id of the best node
	 */
	virtual uint32_t get_node_for_job( const BaseRequest &request, const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const = 0;

	/**
	 * Adds reorganization commands according to the concrete strategy to the given result
	 * @param result the accumulator to add reorg-commands to
	 */
	void reorganize( std::map<uint32_t,NodeReorgDescription> &result );
protected:
	/**
	 * @return the node with the least capacity usage
	 */
	uint32_t get_least_used_node( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;

	/**
	 * Distributes all entries of the underlying cache accross the given nodes
	 * @param result the accumulator to add the entries to
	 * @param all_entries all cache-entries to be distributed
	 */
	virtual void distribute( std::map<uint32_t, ReorgNode> &result, std::vector<std::shared_ptr<const IndexCacheEntry>> &all_entries ) = 0;
private:
	/**
	 * @param nodes the currently active nodes
	 * @return whether the list of currently active nodes changed since the last call to this method
	 */
	bool nodes_changed( const std::map<uint32_t,std::shared_ptr<Node>> &nodes ) const;

	mutable std::set<uint32_t> last_nodes;
	const IndexCache &cache;
	const double max_target_usage;
	const std::unique_ptr<RelevanceFunction> relevance_function;
};

/**
 * Capacity based implementation of the ReorgStrategy.
 * Simply ensures, that all nodes have approx. the same capacity usage.
 */
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


/**
 * Operator-graph based implementation of the ReorgStrategy.
 * Tries to cluster entries by their generating operator-graphs.
 */
class GraphReorgStrategy : public ReorgStrategy {
private:
	/**
	 * Models a node in the operator-graph
	 */
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

/**
 * Geographic based implementation of the ReorgStrategy.
 * Distributes the entries according to their geographic location.
 * The space is dynamically divided using the space-filling z-curve and
 * entries are sorted according to their z-value.
 */
class GeographicReorgStrategy : public ReorgStrategy {
public:
	static uint32_t get_z_value( const BaseCube &c );
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
