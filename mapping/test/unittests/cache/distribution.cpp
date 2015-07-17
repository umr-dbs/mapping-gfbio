/*
 * distribution.cpp
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#include <gtest/gtest.h>
#include <vector>
#include "test/unittests/cache/util.h"
#include "util/make_unique.h"
#include "cache/cache.h"
#include "cache/index/indexserver.h"
#include "cache/node/nodeserver.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/client.h"

typedef std::unique_ptr<std::thread> TP;

class TestIdxServer : public IndexServer {
public:
	void trigger_reorg( uint32_t node_id, const ReorgDescription &desc ) {
		for ( auto &cc : control_connections ) {
			if ( cc.second->node->id == node_id ) {
				cc.second->send_reorg(desc);
				return;
			}
		}
		throw ArgumentException(concat("No node found for id ", node_id));
	}

	TestIdxServer( uint32_t port ) : IndexServer(port) {}
};

class TestNodeServer : public NodeServer {
public:
	TestNodeServer( std::string my_host, uint32_t my_port, std::string index_host, uint32_t index_port ) :
		NodeServer(my_host,my_port,index_host,index_port,1), rcm( 5 * 1024 * 1024, my_host, my_port ) {};
	bool owns_current_thread();
	RemoteCacheManager rcm;
	std::thread::id my_id;
};


class TestCacheMan : public CacheManager {
public:
	const CacheManager& get_instance_mgr( int i ) {
		return instances.at(i)->rcm;
	}

	virtual std::unique_ptr<GenericRaster> query_raster( const GenericOperator &op, const QueryRectangle &rect ) {
		for ( auto i : instances ) {
			if ( i->owns_current_thread() )
				return i->rcm.query_raster(op,rect);
		}
		throw ArgumentException("Unregistered instance called cache-manager");
	}
	virtual std::unique_ptr<GenericRaster> get_raster_local( const std::string &semantic_id, uint64_t entry_id ) const {
		for ( auto i : instances ) {
				if ( i->owns_current_thread() )
					return i->rcm.get_raster_local(semantic_id, entry_id);
		}
		throw ArgumentException("Unregistered instance called cache-manager");
	}
	virtual void put_raster( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster ) {
		for ( auto i : instances ) {
				if ( i->owns_current_thread() )
					return i->rcm.put_raster(semantic_id, raster);
		}
		throw ArgumentException("Unregistered instance called cache-manager");
	}
	virtual STCacheKey put_raster_local( const std::string &semantic_id, const std::unique_ptr<GenericRaster> &raster ) {
		for ( auto i : instances ) {
				if ( i->owns_current_thread() )
					return i->rcm.put_raster_local(semantic_id, raster);
		}
		throw ArgumentException("Unregistered instance called cache-manager");
	}
	virtual void remove_raster_local( const std::string &semantic_id, uint64_t entry_id ) {
		for ( auto i : instances ) {
				if ( i->owns_current_thread() )
					return i->rcm.remove_raster_local(semantic_id, entry_id);
		}
		throw ArgumentException("Unregistered instance called cache-manager");
	}

	void add_instance( TestNodeServer *inst ) {
		instances.push_back( inst );
	}
private:
	std::vector<TestNodeServer*> instances;
};


bool TestNodeServer::owns_current_thread() {
	for ( auto &t : workers ) {
		if ( std::this_thread::get_id() == t->get_id() )
			return true;
	}
	return std::this_thread::get_id() == delivery_thread->get_id() ||
		   std::this_thread::get_id() == my_id;
}

TEST(DistributionTest,TestRedistibution) {


	std::unique_ptr<TestCacheMan> cm = make_unique<TestCacheMan>();
	TestIdxServer is(12346);
	TestNodeServer    ns1( "localhost", 12347, "localhost", 12346 );
	TestNodeServer    ns2( "localhost", 12348, "localhost", 12346 );

	cm->add_instance(&ns1);
	cm->add_instance(&ns2);

	std::unique_ptr<CacheManager> impl = std::move(cm);
	CacheManager::init( impl );

	TestCacheMan &tcm = dynamic_cast<TestCacheMan&>(CacheManager::getInstance());

	std::vector<TP> ts;
	ts.push_back( make_unique<std::thread>(&IndexServer::run, &is) );
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns1.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns2.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));

	ns1.my_id = ts.at(1)->get_id();
	ns2.my_id = ts.at(2)->get_id();

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	CacheClient cc("localhost",12346);

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]), TemporalReference(TIMETYPE_UNIX, timestamp, timestamp), width, height);

	std::string sem_id = GenericOperator::fromJSON(json)->getSemanticId();

	//Should hit 1st node
	cc.get_raster(json,qr,GenericOperator::RasterQM::EXACT);

	try {
		tcm.get_instance_mgr(0).get_raster_local(sem_id,1);
	} catch ( NoSuchElementException &nse ) {
		FAIL();
	}

	ReorgDescription rod;
	ReorgItem ri(ReorgResult::Type::RASTER, "localhost", 12347, sem_id, 1, 1);
	rod.add_item( ri );

	is.trigger_reorg(2, rod);

	std::this_thread::sleep_for( std::chrono::milliseconds(500) );

	// Assert moved
	try {
		tcm.get_instance_mgr(0).get_raster_local(sem_id,1);
		FAIL();
	} catch ( NoSuchElementException &nse ) {
	}

	try {
		tcm.get_instance_mgr(1).get_raster_local(sem_id,1);
	} catch ( NoSuchElementException &nse ) {
		FAIL();
	}


	ns2.stop();
	ns1.stop();
	is.stop();

	for ( TP &t : ts )
		t->join();
}


TEST(DistributionTest,TestRemoteNodeFetch) {


	std::unique_ptr<TestCacheMan> cm = make_unique<TestCacheMan>();
	TestIdxServer     is(12346);
	TestNodeServer    ns1( "localhost", 12347, "localhost", 12346 );
	TestNodeServer    ns2( "localhost", 12348, "localhost", 12346 );

	cm->add_instance(&ns1);
	cm->add_instance(&ns2);

	std::unique_ptr<CacheManager> impl = std::move(cm);
	CacheManager::init( impl );


	std::vector<TP> ts;
	ts.push_back( make_unique<std::thread>(&IndexServer::run, &is) );
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns1.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));
	ts.push_back(ns2.run_async());
	std::this_thread::sleep_for( std::chrono::milliseconds(500));

	ns1.my_id = ts.at(1)->get_id();
	ns2.my_id = ts.at(2)->get_id();

	std::string bbox_str("1252344.2712499984,5009377.085000001,2504688.5424999986,6261721.356250001");
	std::string json = "{\"type\":\"projection\",\"params\":{\"src_projection\":\"EPSG:4326\",\"dest_projection\":\"EPSG:3857\"},\"sources\":{\"raster\":[{\"type\":\"source\",\"params\":{\"sourcename\":\"world1\",\"channel\":0}}]}}";
	std::string timestr("2010-06-06T18:00:00.000Z");
	epsg_t epsg = EPSG_WEBMERCATOR;
	uint32_t width = 256, height = 256;
	time_t timestamp = parseIso8601DateTime(timestr);
	double bbox[4];

	CacheClient cc("localhost",12346);

	parseBBOX(bbox, bbox_str, epsg, false);
	QueryRectangle qr(SpatialReference(epsg, bbox[0], bbox[1], bbox[2], bbox[3]), TemporalReference(TIMETYPE_UNIX, timestamp, timestamp), width, height);

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

