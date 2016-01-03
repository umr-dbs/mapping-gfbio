/*
 * distribution.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#if 0

#include <gtest/gtest.h>
#include <vector>
#include "test/unittests/cache/util.h"
#include "util/make_unique.h"
#include "cache/index/indexserver.h"
#include "cache/node/nodeserver.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/index/reorg_strategy.h"

TEST(DistributionTest,TestRedistibution) {

	TestCacheMan cm;
	TestIdxServer is(12346, "capacity");
	TestNodeServer ns1(12347, "localhost", 12346, "always");
	TestNodeServer ns2(12348, "localhost", 12346, "always");

	cm.add_instance(&ns1);
	cm.add_instance(&ns2);

	CacheManager::init(&cm);

	std::vector<TP> ts;
	ts.push_back(make_unique<std::thread>(&IndexServer::run, &is));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns1));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns2));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json =
		"{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	ClientCacheWrapper<GenericRaster,CacheType::RASTER> cc(CacheType::RASTER, "localhost", 12346);

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
		TemporalReference(TIMETYPE_UNIX, timestamp, timestamp), QueryResolution::pixels(width, height));

	std::string sem_id = GenericOperator::fromJSON(json)->getSemanticId();

	//Should hit 1st node
	auto op = GenericOperator::fromJSON(json);

	cc.query(*op, qr);

	NodeCacheKey key1(sem_id, 2);

	try {
		cm.get_instance_mgr(0).get_raster_cache().get_ref(key1);
	} catch (NoSuchElementException &nse) {
		FAIL();
	}

	ReorgDescription rod;
	ReorgMoveItem ri(CacheType::RASTER, sem_id, 2, 1, "localhost", 12347);
	rod.add_move(ri);

	is.trigger_reorg(2, rod);

	std::this_thread::sleep_for(std::chrono::milliseconds(2500));

	// Assert moved
	try {
		cm.get_instance_mgr(0).get_raster_cache().get_ref(key1);
		FAIL();
	} catch (NoSuchElementException &nse) {
	}

	NodeCacheKey key_new(sem_id, 1);
	try {
		cm.get_instance_mgr(1).get_raster_cache().get_ref(key_new);
	} catch (NoSuchElementException &nse) {
		FAIL();
	}

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();

	Log::error("DONE");
}

TEST(DistributionTest,TestRemoteNodeFetch) {
	// Reset testnodeutil
	TestCacheMan cm;
	TestIdxServer is(12346, "capacity");
	TestNodeServer ns1(12347, "localhost", 12346, "always");
	TestNodeServer ns2(12348, "localhost", 12346, "always");

	cm.add_instance(&ns1);
	cm.add_instance(&ns2);

	CacheManager::init(&cm);

	std::vector<TP> ts;
	ts.push_back(make_unique<std::thread>(&IndexServer::run, &is));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns1));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns2));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json =
		"{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	ClientCacheWrapper<GenericRaster,CacheType::RASTER> cc(CacheType::RASTER, "localhost", 12346);

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
		TemporalReference(TIMETYPE_UNIX, timestamp, timestamp), QueryResolution::pixels(width, height));


	auto op = GenericOperator::fromJSON(json);
	//Should hit 1st node
	cc.query(*op, qr);

	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	//Should hit 2nd node
	cc.query(*op, qr);

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();
}

TEST(DistributionTest,TestCapacityReorg) {

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

	ASSERT_TRUE(cache.requires_reorg(nodes));
	cache.reorganize(res);

	ASSERT_TRUE(res.at(2).node->id == 2);
	ASSERT_TRUE(res.at(2).get_moves().size() == 1);
	ASSERT_TRUE(res.at(2).get_moves().at(0).entry_id == 1);
	ASSERT_TRUE(res.at(2).get_removals().empty());

	ASSERT_TRUE(res.at(1).node->id == 1);
	ASSERT_TRUE(res.at(1).is_empty());
}


TEST(DistributionTest,TestGeographicReorg) {

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

	ASSERT_TRUE(cache.requires_reorg(nodes));
	cache.reorganize(res);

	ASSERT_TRUE(res.at(2).node->id == 2);
	Log::error("Moves/Removes: %d/%d", res.at(1).get_moves().size(), res.at(1).get_removals().size());
	Log::error("Moves/Removes: %d/%d", res.at(2).get_moves().size(), res.at(2).get_removals().size());
	ASSERT_TRUE(res.at(2).get_moves().size() == 1);
	ASSERT_TRUE(res.at(2).get_moves().at(0).entry_id == 1);
	ASSERT_TRUE(res.at(2).get_removals().empty());

	ASSERT_TRUE(res.at(1).node->id == 1);
	ASSERT_TRUE(res.at(1).is_empty());
}




TEST(DistributionTest,TestStatsAndReorg) {
	// Reset testnodeutil
	TestCacheMan cm;
	TestIdxServer is(12346, "capacity");
	TestNodeServer ns1(12347, "localhost", 12346, "always", 204800 );
	TestNodeServer ns2(12348, "localhost", 12346, "always", 204800 );

	cm.add_instance(&ns1);
	cm.add_instance(&ns2);

	CacheManager::init(&cm);

	std::vector<TP> ts;
	ts.push_back(make_unique<std::thread>(&IndexServer::run, &is));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns1));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	ts.push_back(make_unique<std::thread>(TestNodeServer::run_node_thread, &ns2));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	std::string json = "{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_LATLON;

	time_t timestamp = parseIso8601DateTime(timestr);

	auto op = GenericOperator::fromJSON(json);

	ClientCacheWrapper<GenericRaster,CacheType::RASTER> cc(CacheType::RASTER, "localhost", 12346);

	TemporalReference tr(TIMETYPE_UNIX, timestamp, timestamp);
	QueryResolution   qres = QueryResolution::pixels(256, 256);


	QueryRectangle qr1( SpatialReference(epsg, 0,0,45,45), tr, qres );
	QueryRectangle qr2( SpatialReference(epsg, 45,0,90,45), tr, qres );

	//Should hit 1st node
	cc.query(*op, qr1);
	cc.query(*op, qr2);
	cc.query(*op, qr2);

	is.force_stat_update();

	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	// Reorg should be finished at this point

	// Assert moved
	try {
		NodeCacheKey k(op->getSemanticId(),2);
		cm.get_instance_mgr(0).get_raster_cache().get_ref(k);
		Log::debug("FAILED on get 2");
		FAIL();
	} catch (NoSuchElementException &nse) {
	}

	try {
		NodeCacheKey k(op->getSemanticId(),1);
		cm.get_instance_mgr(0).get_raster_cache().get_ref(k);
	} catch (NoSuchElementException &nse) {
		FAIL();
	}

	try {
		NodeCacheKey k(op->getSemanticId(),1);
		cm.get_instance_mgr(1).get_raster_cache().get_ref(k);
	} catch (NoSuchElementException &nse) {
		FAIL();
	}

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();
}

#endif
