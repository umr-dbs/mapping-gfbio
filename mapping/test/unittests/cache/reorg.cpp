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

TEST(ReorgTest,CapacityReorg) {

	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", 42, Capacity(30, 0,0,0,0,0,0,0,0,0)));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", 4711, Capacity(30, 0,0,0,0,0,0,0,0,0)));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexRasterCache cache("capacity");

	// Entry 1
	NodeCacheKey k1("key", 1);
	CacheCube b1(SpatialReference(EPSG_LATLON, 0, 0, 45, 45), TemporalReference(TIMETYPE_UNIX, 0, 10));
	CacheEntry c1(b1, 10, 3.0);
	NodeCacheRef r1(CacheType::RASTER, k1, c1);
	IndexCacheEntry e1(1, r1);

	// Entry 2
	NodeCacheKey k2("key", 2);
	CacheCube b2(SpatialReference(EPSG_LATLON, 45, 0, 90, 45), TemporalReference(TIMETYPE_UNIX, 0, 10));
	CacheEntry c2(b2, 10, 3.0);
	NodeCacheRef r2(CacheType::RASTER, k2, c2);
	IndexCacheEntry e2(1, r2);

	// Increase count
	n1->capacity.raster_cache_used = 20;
	e2.access_count = 2;

	cache.put(e1);
	cache.put(e2);

	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	cache.reorganize(res);
	EXPECT_EQ(2,res.at(2).node->id);
	EXPECT_EQ(1,res.at(2).get_moves().size());
	EXPECT_EQ(1,res.at(2).get_moves().at(0).entry_id);
	EXPECT_TRUE(res.at(2).get_removals().empty());

	EXPECT_EQ(1, res.at(1).node->id);
	EXPECT_TRUE(res.at(1).is_empty());
}


TEST(ReorgTest,GeographicReorg) {

	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", 42, Capacity(40, 0,0,0,0,0,0,0,0,0)));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", 4711, Capacity(40, 0,0,0,0,0,0,0,0,0)));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexRasterCache cache("geo");

	// Entry 1
	NodeCacheKey k1("key", 1);
	CacheCube b1(SpatialReference(EPSG_LATLON, 0, 0, 45, 45), TemporalReference(TIMETYPE_UNIX, 0, 10));
	CacheEntry c1(b1, 10, 3.0);
	NodeCacheRef r1(CacheType::RASTER, k1, c1);
	IndexCacheEntry e1(1, r1);

	// Entry 2
	NodeCacheKey k2("key", 2);
	CacheCube b2(SpatialReference(EPSG_LATLON, 45, 0, 90, 45),
		TemporalReference(TIMETYPE_UNIX, 0, 10));
	CacheEntry c2(b2, 10, 3.0);
	NodeCacheRef r2(CacheType::RASTER, k2, c2);
	IndexCacheEntry e2(1, r2);

	// Increase count
	n1->capacity.raster_cache_used = 20;
	e2.access_count = 2;

	cache.put(e1);
	cache.put(e2);

	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	cache.reorganize(res);

	EXPECT_EQ(2,res.at(2).node->id);
	Log::error("Moves/Removes: %d/%d", res.at(1).get_moves().size(), res.at(1).get_removals().size());
	Log::error("Moves/Removes: %d/%d", res.at(2).get_moves().size(), res.at(2).get_removals().size());
	EXPECT_EQ(1,res.at(2).get_moves().size());
	EXPECT_EQ(2,res.at(2).get_moves().at(0).entry_id);
	EXPECT_TRUE(res.at(2).get_removals().empty());

	EXPECT_EQ(1,res.at(1).node->id);
	EXPECT_TRUE(res.at(1).is_empty());
}

IndexCacheEntry createGraphEntry( uint32_t node_id, uint64_t entry_id, const std::string &workflow, size_t size) {
	NodeCacheKey k1(workflow, entry_id);
	CacheCube b1(SpatialReference(EPSG_LATLON, 0, 0, 180, 90), TemporalReference(TIMETYPE_UNIX, 0, 10));
	CacheEntry c1(b1, size, 1.0);
	NodeCacheRef r1(CacheType::RASTER, k1, c1);
	return IndexCacheEntry(node_id, r1);
}

TEST(ReorgTest,GraphReorg) {
	std::shared_ptr<Node> n1 = std::shared_ptr<Node>(new Node(1, "localhost", 42, Capacity(40, 0,0,0,0,0,0,0,0,0)));
	std::shared_ptr<Node> n2 = std::shared_ptr<Node>(new Node(2, "localhost", 4711, Capacity(40, 0,0,0,0,0,0,0,0,0)));

	std::map<uint32_t, std::shared_ptr<Node>> nodes;
	nodes.emplace(1, n1);
	nodes.emplace(2, n2);

	IndexRasterCache cache("graph");
	std::map<uint32_t, NodeReorgDescription> res;
	for ( auto &kv : nodes ) {
		res.emplace(kv.first, NodeReorgDescription(kv.second));
	}

	auto e1 = createGraphEntry(1, 1, "SRC", 5 );
	auto e2 = createGraphEntry(1, 2, "SRC", 5 );
	auto e3 = createGraphEntry(1, 3, "SRC", 5 );
	auto e4 = createGraphEntry(1, 4, "OP1 {SRC}", 3 );
	auto e5 = createGraphEntry(1, 5, "OP1 {SRC}", 3 );
	auto e6 = createGraphEntry(1, 6, "OP1 {SRC}", 3 );
	auto e7 = createGraphEntry(1, 7, "OP1 {SRC}", 3 );
	auto e8 = createGraphEntry(1, 8, "OP2 {SRC}", 2 );

	n1->capacity = Capacity(40, 29,0,0,0,0,0,0,0,0);

	cache.put(e1);
	cache.put(e2);
	cache.put(e3);
	cache.put(e4);
	cache.put(e5);
	cache.put(e6);
	cache.put(e7);
	cache.put(e8);

	cache.reorganize(res);

	EXPECT_EQ(4,res.at(2).get_moves().size());
	EXPECT_EQ(1,res.at(2).get_moves().at(0).entry_id);
	EXPECT_EQ(2,res.at(2).get_moves().at(1).entry_id);
	EXPECT_EQ(3,res.at(2).get_moves().at(2).entry_id);
	EXPECT_EQ(8,res.at(2).get_moves().at(3).entry_id);
	EXPECT_EQ(0,res.at(2).get_removals().size());

	EXPECT_TRUE(res.at(1).get_moves().empty());
	EXPECT_TRUE(res.at(1).get_removals().empty());

}
