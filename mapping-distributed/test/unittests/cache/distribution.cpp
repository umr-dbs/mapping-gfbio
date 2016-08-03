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
#include "cache/priv/redistribution.h"
#include "cache/index/reorg_strategy.h"
#include "cache/experiments/exp_util.h"
#include "datatypes/raster.h"

typedef std::unique_ptr<std::thread> TP;

TEST(DistributionTest,TestRedistibution) {
	Log::setLevel(Log::LogLevel::WARN);
	Configuration::loadFromDefaultPaths();

	LocalTestSetup stp(2,1,0,50*1024*1024,"capacity","lru","always", 12346);

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json =
		"{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"rasterdb_source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	auto &cc = stp.get_client().get_raster_cache();

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
		TemporalReference(TIMETYPE_UNIX, timestamp, timestamp+1), QueryResolution::pixels(width, height));

	std::string sem_id = GenericOperator::fromJSON(json)->getSemanticId();

	//Should hit 1st node
	QueryProfiler qp;
	auto op = GenericOperator::fromJSON(json);
	cc.query(*op, qr, qp);

	NodeCacheKey key1(sem_id, 2);

	int s_id = 1;
	int d_id = 2;

	try {
		stp.get_node(s_id).get_cache_manager().get_raster_cache().get(key1);
	} catch ( const NoSuchElementException &nse ) {
		EXPECT_NO_THROW(stp.get_node(d_id).get_cache_manager().get_raster_cache().get(key1));
		std::swap(s_id,d_id);
	}

	auto &s_node = stp.get_node(s_id);
	auto &d_node = stp.get_node(d_id);


	ReorgDescription rod;
	ReorgMoveItem ri(CacheType::RASTER, key1.semantic_id, s_node.get_id(), key1.entry_id, s_node.get_host(), s_node.get_port());
	rod.add_move(ri);

	stp.get_index().trigger_reorg(d_node.get_id(),rod);
	stp.get_index().force_stat_update();

	// Assert moved
	EXPECT_THROW(s_node.get_cache_manager().get_raster_cache().get(key1), NoSuchElementException);

	NodeCacheKey key_new(sem_id, 1);
	EXPECT_NO_THROW(d_node.get_cache_manager().get_raster_cache().get(key_new));
}

TEST(DistributionTest,TestRemoteNodeFetch) {
	// Reset testnodeutil
	Configuration::loadFromDefaultPaths();
	LocalTestSetup stp(2,1,0,50*1024*1024,"capacity","lru","always", 12346);

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json =
		"{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"rasterdb_source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	auto &cc = stp.get_client().get_raster_cache();

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]),
		TemporalReference(TIMETYPE_UNIX, timestamp, timestamp+1), QueryResolution::pixels(width, height));


	auto op = GenericOperator::fromJSON(json);
	QueryProfiler qp1, qp2;
	//Should hit 1st node
	cc.query(*op, qr, qp1);

	stp.get_index().force_stat_update();

	//Should hit 2nd node
	cc.query(*op, qr, qp2);
}

TEST(DistributionTest,TestStatsAndReorg) {
	// Reset testnodeutil
//	Log::setLevel(Log::LogLevel::DEBUG);

	Configuration::loadFromDefaultPaths();
	LocalTestSetup stp(2,1,500,204800,"capacity","lru","always", 12346);

	std::string json = "{\"type\":\"rasterdb_source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_LATLON;

	time_t timestamp = parseIso8601DateTime(timestr);

	auto op = GenericOperator::fromJSON(json);

	auto &cc = stp.get_client().get_raster_cache();

	TemporalReference tr(TIMETYPE_UNIX, timestamp, timestamp+1);
	QueryResolution   qres = QueryResolution::pixels(256, 256);


	QueryRectangle qr1( SpatialReference(epsg, 0,0,45,45), tr, qres );
	QueryRectangle qr2( SpatialReference(epsg, 45,0,90,45), tr, qres );
	QueryProfiler qp1, qp2, qp3;

	//Should hit 1st node
	cc.query(*op, qr1, qp1);
	cc.query(*op, qr2, qp2);
	cc.query(*op, qr2, qp3);


	stp.get_index().force_reorg();

	try {
		stp.get_node(1).get_cache_manager().get_raster_cache().get(NodeCacheKey(op->getSemanticId(),1));
		stp.get_node(2).get_cache_manager().get_raster_cache().get(NodeCacheKey(op->getSemanticId(),2));
	} catch ( const NoSuchElementException &nse ) {
		EXPECT_NO_THROW(stp.get_node(1).get_cache_manager().get_raster_cache().get(NodeCacheKey(op->getSemanticId(),2)));
		EXPECT_NO_THROW(stp.get_node(2).get_cache_manager().get_raster_cache().get(NodeCacheKey(op->getSemanticId(),1)));
	}

}

#endif
