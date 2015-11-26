/*
 * util.h
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#ifndef UNITTESTS_CACHE_UTIL_H_
#define UNITTESTS_CACHE_UTIL_H_

#include "cache/index/indexserver.h"
#include "cache/node/nodeserver.h"
#include "cache/manager.h"
#include "datatypes/spatiotemporal.h"
#include "util/exceptions.h"

#include <iostream>
#include <string>
#include <cmath>
#include <thread>
#include <memory>

/**
 * This function converts a "datetime"-string in ISO8601 format into a time_t using UTC
 * @param dateTimeString a string with ISO8601 "datetime"
 * @returns The time_t representing the "datetime"
 */
time_t parseIso8601DateTime(std::string dateTimeString);

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg = EPSG_WEBMERCATOR, bool allow_infinite = false);


typedef std::unique_ptr<std::thread> TP;

class TestIdxServer : public IndexServer {
public:
	void trigger_reorg( uint32_t node_id, const ReorgDescription &desc );

	void force_stat_update();

	TestIdxServer( uint32_t port, const std::string &reorg_strategy );
};

class TestNodeServer : public NodeServer {
public:
	static void run_node_thread(TestNodeServer *ns);

	TestNodeServer(const std::string &my_host, uint32_t my_port, const std::string &index_host, uint32_t index_port, const std::string &strategy, size_t capacity = 5 * 1024 * 1024 );
	bool owns_current_thread();
	NodeCacheManager rcm;
	std::thread::id my_id;
};


class TestCacheMan : public CacheManager {
public:
	void add_instance( TestNodeServer *inst );
	CacheManager& get_instance_mgr( int i );


	// Creates a handshake message for the index-server
	NodeHandshake get_handshake() const;

	// Retrieves statistics for this cache
	NodeStats get_stats() const;

	CacheWrapper<GenericRaster>& get_raster_cache();
	CacheWrapper<PointCollection>& get_point_cache();
	CacheWrapper<LineCollection>& get_line_cache();
	CacheWrapper<PolygonCollection>& get_polygon_cache();
	CacheWrapper<GenericPlot>& get_plot_cache();
	CachingStrategy& get_strategy();
private:
	CacheManager& get_current_instance() const;
	std::vector<TestNodeServer*> instances;
};

#endif /* UNITTESTS_CACHE_UTIL_H_ */
