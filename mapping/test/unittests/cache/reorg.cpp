/*
 * reorg.cpp
 *
 *  Created on: 04.01.2016
 *      Author: mika
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <map>
#include "cache/priv/redistribution.h"
#include "cache/index/reorg_strategy.h"
#include "cache/index/indexserver.h"
#include "cache/index/query_manager/simple_query_manager.h"

TEST(ReorgTest,CapacityReorg) {

	NodeHandshake h1(42, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 30, 20) } );
	NodeHandshake h2(4711, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 30, 0) } );

	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", h1, std::unique_ptr<ControlConnection>()));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", h2, std::unique_ptr<ControlConnection>()));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexCache cache(CacheType::RASTER);
	auto reorg = ReorgStrategy::by_name(cache,"capacity","lru");

	// Entry 1
	CacheCube b1(SpatioTemporalReference(SpatialReference(EPSG_LATLON, 0, 0, 45, 45), TemporalReference(TIMETYPE_UNIX, 0, 10)));
	CacheEntry c1(b1, 10, ProfilingData());
	cache.put("key",1,1,c1);

	// Entry 2
	CacheCube b2(SpatioTemporalReference(SpatialReference(EPSG_LATLON, 45, 0, 90, 45), TemporalReference(TIMETYPE_UNIX, 0, 10)));
	CacheEntry c2(b2, 10, ProfilingData());
	cache.put("key",1,2,c2);

	CacheStats cs(CacheType::RASTER,40,20);
	cs.add_item("key", NodeEntryStats(2,CacheCommon::time_millis(),2));

	// Increase count
	cache.update_stats(1, cs );

	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	DemaQueryManager dqm(nodes);
	reorg->reorganize(res);
	EXPECT_EQ(2,res.at(2).node->id);
	EXPECT_EQ(1,res.at(2).get_moves().size());
//	EXPECT_EQ(1,res.at(2).get_moves().at(0).entry_id);
	EXPECT_TRUE(res.at(2).get_removals().empty());

	EXPECT_EQ(1, res.at(1).node->id);
	EXPECT_TRUE(res.at(1).is_empty());
}


TEST(ReorgTest,GeographicReorg) {

	NodeHandshake h1(42, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 40, 20) } );
	NodeHandshake h2(4711, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 40, 0) } );

	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", h1, std::unique_ptr<ControlConnection>()));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", h2, std::unique_ptr<ControlConnection>()));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexCache cache(CacheType::RASTER);
	auto reorg = ReorgStrategy::by_name(cache,"geo","lru");

	// Entry 1
	CacheCube b1(SpatioTemporalReference(SpatialReference(EPSG_LATLON, 0, 0, 45, 45), TemporalReference(TIMETYPE_UNIX, 0, 10)));
	CacheEntry c1(b1, 10, ProfilingData());

	cache.put("key",1,1,c1);

	// Entry 2
	CacheCube b2(SpatioTemporalReference(SpatialReference(EPSG_LATLON, 45, 0, 90, 45), TemporalReference(TIMETYPE_UNIX, 0, 10)));
	CacheEntry c2(b2, 10, ProfilingData());

	cache.put("key",1,2,c2);


	CacheStats cs(CacheType::RASTER,40,20);
	cs.add_item("key", NodeEntryStats(2,CacheCommon::time_millis(),2));

	// Increase count
	cache.update_stats(1, cs );


	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	reorg->reorganize(res);

	EXPECT_EQ(2,res.at(2).node->id);
	EXPECT_EQ(1,res.at(2).get_moves().size());
	EXPECT_EQ(2,res.at(2).get_moves().at(0).entry_id);
	EXPECT_TRUE(res.at(2).get_removals().empty());

	EXPECT_EQ(1,res.at(1).node->id);
	EXPECT_TRUE(res.at(1).is_empty());
}

CacheEntry createGraphEntry( size_t size) {
	CacheCube b1(SpatioTemporalReference(SpatialReference(EPSG_LATLON, 0, 0, 180, 90), TemporalReference(TIMETYPE_UNIX, 0, 10)));
	return CacheEntry(b1, size, ProfilingData());
}

TEST(ReorgTest,GraphReorg) {
	NodeHandshake h1(42, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 40, 29) } );
	NodeHandshake h2(4711, std::vector<CacheHandshake>{ CacheHandshake(CacheType::RASTER, 40, 0) } );

	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", h1, std::unique_ptr<ControlConnection>() ));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", h2, std::unique_ptr<ControlConnection>() ));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexCache cache(CacheType::RASTER);
	auto reorg = ReorgStrategy::by_name(cache,"graph","lru");
	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	cache.put("SRC", 1, 1, createGraphEntry(5) );
	cache.put("SRC", 1, 2, createGraphEntry(5) );
	cache.put("SRC", 1, 3, createGraphEntry(5) );
	cache.put("OP1 {SRC}", 1, 4, createGraphEntry(3) );
	cache.put("OP1 {SRC}", 1, 5, createGraphEntry(3) );
	cache.put("OP1 {SRC}", 1, 6, createGraphEntry(3) );
	cache.put("OP1 {SRC}", 1, 7, createGraphEntry(3) );
	cache.put("OP2 {SRC}", 1, 8, createGraphEntry(2) );


	reorg->reorganize(res);

	EXPECT_EQ(4,res.at(2).get_moves().size());
//	EXPECT_EQ(3,res.at(2).get_moves().at(0).entry_id);
//	EXPECT_EQ(1,res.at(2).get_moves().at(1).entry_id);
//	EXPECT_EQ(2,res.at(2).get_moves().at(2).entry_id);
//	EXPECT_EQ(8,res.at(2).get_moves().at(3).entry_id);
	EXPECT_EQ(0,res.at(2).get_removals().size());

	EXPECT_TRUE(res.at(1).get_moves().empty());
	EXPECT_TRUE(res.at(1).get_removals().empty());

}
