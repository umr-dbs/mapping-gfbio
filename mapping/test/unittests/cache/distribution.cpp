/*
 * distribution.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#if 0

#include <gtest/gtest.h>
#include <vector>
#include "util/make_unique.h"
#include "cache/index/indexserver.h"
#include "cache/node/nodeserver.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/index/reorg_strategy.h"
#include "cache/experiments/exp_util.h"

typedef std::unique_ptr<std::thread> TP;

TEST(DistributionTest,TestRedistibution) {
	Log::setLevel(Log::LogLevel::WARN);
	TestCacheMan cm;
	TestIdxServer is(12346, 0, "capacity", "lru");
	TestNodeServer ns1(1, 12347, "localhost", 12346, "always");
	TestNodeServer ns2(1, 12348, "localhost", 12346, "always");

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
	QueryProfiler qp;
	auto op = GenericOperator::fromJSON(json);
	cc.query(*op, qr, qp);

	NodeCacheKey key1(sem_id, 2);

	EXPECT_NO_THROW(cm.get_instance_mgr(0).get_raster_cache().get(key1));

	ReorgDescription rod;
	ReorgMoveItem ri(CacheType::RASTER, sem_id, 2, 1, "localhost", 12347);
	rod.add_move(ri);

	is.trigger_reorg(2, rod);

	std::this_thread::sleep_for(std::chrono::milliseconds(2500));

	// Assert moved
	EXPECT_THROW(cm.get_instance_mgr(0).get_raster_cache().get(key1), NoSuchElementException);

	NodeCacheKey key_new(sem_id, 1);
	EXPECT_NO_THROW(cm.get_instance_mgr(1).get_raster_cache().get(key_new));

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();
}

TEST(DistributionTest,TestRemoteNodeFetch) {
	// Reset testnodeutil
	TestCacheMan cm;
	TestIdxServer is(12346, 0, "capacity", "lru");
	TestNodeServer ns1(1, 12347, "localhost", 12346, "always");
	TestNodeServer ns2(1, 12348, "localhost", 12346, "always");

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
	QueryProfiler qp1, qp2;
	//Should hit 1st node
	cc.query(*op, qr, qp1);

	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	//Should hit 2nd node
	cc.query(*op, qr, qp2);

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();
}

TEST(DistributionTest,TestStatsAndReorg) {
	// Reset testnodeutil
	TestCacheMan cm;
	TestIdxServer is(12346, 500, "capacity", "lru");
	TestNodeServer ns1(1, 12347, "localhost", 12346, "always", 204800 );
	TestNodeServer ns2(1, 12348, "localhost", 12346, "always", 204800 );

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
	QueryProfiler qp1, qp2, qp3;

	//Should hit 1st node
	cc.query(*op, qr1, qp1);
	cc.query(*op, qr2, qp2);
	cc.query(*op, qr2, qp3);

	is.force_stat_update();

	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	// Reorg should be finished at this point

	// Assert moved
	EXPECT_THROW(cm.get_instance_mgr(0).get_raster_cache().get(NodeCacheKey(op->getSemanticId(),2)),
			NoSuchElementException );

	EXPECT_NO_THROW(cm.get_instance_mgr(0).get_raster_cache().get(NodeCacheKey(op->getSemanticId(),1)));
	EXPECT_NO_THROW(cm.get_instance_mgr(1).get_raster_cache().get(NodeCacheKey(op->getSemanticId(),1)));

	ns2.stop();
	ns1.stop();
	is.stop();

	for (TP &t : ts)
		t->join();
}

#endif
