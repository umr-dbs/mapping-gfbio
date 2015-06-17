/*
 * distribution.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include "test/unittests/cache/util.h"
#include "cache/cache.h"
#include "cache/index/indexserver.h"
#include "cache/node/nodeserver.h"
#include "cache/client.h"

typedef std::unique_ptr<std::thread> TP;

class TestIdxServer : public IndexServer {
public:
	TestIdxServer( uint32_t frontend_port, uint32_t node_port ) : IndexServer(frontend_port,node_port) {}
protected:
	virtual NP get_node_for_job(const std::unique_ptr<CacheRequest> &request);
private:
	uint64_t last_node = 0;
};

class TestCacheMan : public CacheManager {
	static thread_local RemoteCacheManager *rcm;
public:
	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) {
		return TestCacheMan::rcm->query_raster(op, rect);
	}
	virtual std::unique_ptr<GenericRaster> get_raster( const std::string &semantic_id, uint64_t entry_id ) {
		return TestCacheMan::rcm->get_raster(semantic_id, entry_id);
	}
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster ) {
		TestCacheMan::rcm->put_raster(semantic_id, raster);
	}
};

thread_local RemoteCacheManager *TestCacheMan::rcm = new RemoteCacheManager(5*1024*1024);


IndexServer::NP TestIdxServer::get_node_for_job(const std::unique_ptr<CacheRequest>& request) {
	(void) request;
	int node = last_node++ % nodes.size();

	auto it = nodes.begin();
	while ( node > 0 ) {
		++it;
		node--;
	}

	Log::debug("Picking node: %d", it->second->id);

	return it->second;
}

TEST(DistributionTest,TestRemoteNodeFetch) {

	std::unique_ptr<CacheManager> impl = std::make_unique<TestCacheMan>();
	CacheManager::init( impl );

	TestIdxServer is(12346,12347);
	NodeServer    ns1( "localhost", 12348, "localhost", 12347, 1);
	NodeServer    ns2( "localhost", 12349, "localhost", 12347, 1);
	std::vector<TP> ts;
	ts.push_back(is.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns1.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns2.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	CacheClient cc("localhost",12346);

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(timestamp, bbox[0], bbox[1], bbox[2], bbox[3], width, height, epsg);

	//Should hit 1st node
	cc.get_raster(json,qr,GenericOperator::RasterQM::EXACT);


	std::this_thread::sleep_for( std::chrono::milliseconds(500));

	//Should hit 2nd node
	cc.get_raster(json,qr,GenericOperator::RasterQM::EXACT);

	ns2.stop();
	ns1.stop();
	is.stop();

	for ( TP &t : ts )
		t->join();
}
