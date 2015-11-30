/*
 * util.cpp
 *
 *  Created on: 18.05.2015
 *      Author: mika
 */

#include "test/unittests/cache/util.h"
#include <chrono>

time_t parseIso8601DateTime(std::string dateTimeString) {
	const std::string dateTimeFormat { "%Y-%m-%dT%H:%M:%S" }; //TODO: we should allow millisec -> "%Y-%m-%dT%H:%M:%S.SSSZ" std::get_time and the tm struct dont have them.

	//std::stringstream dateTimeStream{dateTimeString}; //TODO: use this with gcc >5.0
	tm queryDateTime;
	//std::get_time(&queryDateTime, dateTimeFormat); //TODO: use this with gcc >5.0
	strptime(dateTimeString.c_str(), dateTimeFormat.c_str(), &queryDateTime); //TODO: remove this with gcc >5.0
	time_t queryTimestamp = timegm(&queryDateTime); //TODO: is there a c++ version for timegm?

	//TODO: parse millisec

	return (queryTimestamp);
}

void parseBBOX(double *bbox, const std::string bbox_str, epsg_t epsg, bool allow_infinite) {
	// &BBOX=0,0,10018754.171394622,10018754.171394622
	for (int i = 0; i < 4; i++)
		bbox[i] = NAN;

	// Figure out if we know the extent of the CRS
	// WebMercator, http://www.easywms.com/easywms/?q=en/node/3592
	//                               minx          miny         maxx         maxy
	double extent_webmercator[4] { -20037508.34, -20037508.34, 20037508.34, 20037508.34 };
	double extent_latlon[4] { -180, -90, 180, 90 };
	double extent_msg[4] { -5568748.276, -5568748.276, 5568748.276, 5568748.276 };

	double *extent = nullptr;
	if (epsg == EPSG_WEBMERCATOR)
		extent = extent_webmercator;
	else if (epsg == EPSG_LATLON)
		extent = extent_latlon;
	else if (epsg == EPSG_GEOSMSG)
		extent = extent_msg;

	std::string delimiters = " ,";
	size_t current, next = -1;
	int element = 0;
	do {
		current = next + 1;
		next = bbox_str.find_first_of(delimiters, current);
		std::string stringValue = bbox_str.substr(current, next - current);
		double value = 0;

		if (stringValue == "Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::max(extent[element], extent[(element + 2) % 4]);
		}
		else if (stringValue == "-Infinity") {
			if (!allow_infinite)
				throw ArgumentException("cannot process BBOX with Infinity");
			if (!extent)
				throw ArgumentException("cannot process BBOX with Infinity and unknown CRS");
			value = std::min(extent[element], extent[(element + 2) % 4]);
		}
		else {
			value = std::stod(stringValue);
			if (!std::isfinite(value))
				throw ArgumentException("BBOX contains entry that is not a finite number");
		}

		bbox[element++] = value;
	} while (element < 4 && next != std::string::npos);

	if (element != 4)
		throw ArgumentException("Could not parse BBOX parameter");

	/*
	 * OpenLayers insists on sending latitude in x and longitude in y.
	 * The MAPPING code (including gdal's projection classes) don't agree: east/west should be in x.
	 * The simple solution is to swap the x and y coordinates.
	 * OpenLayers 3 uses the axis orientation of the projection to determine the bbox axis order. https://github.com/openlayers/ol3/blob/master/src/ol/source/imagewmssource.js ~ line 317.
	 */
	if (epsg == EPSG_LATLON) {
		std::swap(bbox[0], bbox[1]);
		std::swap(bbox[2], bbox[3]);
	}

	// If no extent is known, just trust the client.
	if (extent) {
		double bbox_normalized[4];
		for (int i = 0; i < 4; i += 2) {
			bbox_normalized[i] = (bbox[i] - extent[0]) / (extent[2] - extent[0]);
			bbox_normalized[i + 1] = (bbox[i + 1] - extent[1]) / (extent[3] - extent[1]);
		}

		// Koordinaten können leicht ausserhalb liegen, z.B.
		// 20037508.342789, 20037508.342789
		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 && bbox_normalized[i] > -0.001)
				bbox_normalized[i] = 0.0;
			else if (bbox_normalized[i] > 1.0 && bbox_normalized[i] < 1.001)
				bbox_normalized[i] = 1.0;
		}

		for (int i = 0; i < 4; i++) {
			if (bbox_normalized[i] < 0.0 || bbox_normalized[i] > 1.0)
				throw ArgumentException("BBOX exceeds extent");
		}
	}

	//bbox_normalized[1] = 1.0 - bbox_normalized[1];
	//bbox_normalized[3] = 1.0 - bbox_normalized[3];
}

//
// Test extensions
//

TestNodeServer::TestNodeServer(uint32_t my_port, const std::string &index_host, uint32_t index_port, const std::string &strategy, size_t capacity)  :
	NodeServer(my_port,index_host,index_port,1),
	rcm( CachingStrategy::by_name(strategy), capacity,capacity,capacity,capacity,capacity ) {
	rcm.set_self_port(my_port);
}

bool TestNodeServer::owns_current_thread() {
	for ( auto &t : workers ) {
		if ( std::this_thread::get_id() == t->get_id() )
			return true;
	}
	return (delivery_thread != nullptr && std::this_thread::get_id() == delivery_thread->get_id()) ||
		    std::this_thread::get_id() == my_id;
}

void TestNodeServer::run_node_thread(TestNodeServer* ns) {
	ns->my_id = std::this_thread::get_id();
	ns->run();
}

// Test index

TestIdxServer::TestIdxServer(uint32_t port, const std::string &reorg_strategy)  : IndexServer(port,reorg_strategy) {
}

void TestIdxServer::trigger_reorg(uint32_t node_id, const ReorgDescription& desc)  {
	for ( auto &cc : control_connections ) {
		if ( cc.second->node->id == node_id ) {
			cc.second->send_reorg(desc);
			return;
		}
	}
	throw ArgumentException(concat("No node found for id ", node_id));
}

//
// Test Cache Manager
//

CacheManager& TestCacheMan::get_instance_mgr(int i) {
	return instances.at(i)->rcm;
}

NodeHandshake TestCacheMan::get_handshake(uint32_t my_port) const {
	return get_current_instance().get_handshake(my_port);
}

NodeStats TestCacheMan::get_stats() const {
	return get_current_instance().get_stats();
}

CacheWrapper<GenericRaster>& TestCacheMan::get_raster_cache() {
	return get_current_instance().get_raster_cache();
}

CacheWrapper<PointCollection>& TestCacheMan::get_point_cache() {
	return get_current_instance().get_point_cache();
}

CacheWrapper<LineCollection>& TestCacheMan::get_line_cache() {
	return get_current_instance().get_line_cache();
}

CacheWrapper<PolygonCollection>& TestCacheMan::get_polygon_cache() {
	return get_current_instance().get_polygon_cache();
}

CacheWrapper<GenericPlot>& TestCacheMan::get_plot_cache() {
	return get_current_instance().get_plot_cache();
}

void TestCacheMan::add_instance(TestNodeServer *inst) {
	instances.push_back(inst);
}

void TestCacheMan::set_self_port(uint32_t port) {
	get_current_instance().set_self_port(port);
}

void TestCacheMan::set_self_host(const std::string& host) {
	get_current_instance().set_self_host(host);
}

CacheRef TestCacheMan::create_self_ref(uint64_t id) {
	return get_current_instance().create_self_ref(id);
}

bool TestCacheMan::is_self_ref(const CacheRef& ref) {
	return get_current_instance().is_self_ref(ref);
}

CacheManager& TestCacheMan::get_current_instance() const {
	for (auto i : instances) {
		if (i->owns_current_thread())
			return i->rcm;
	}
	throw ArgumentException("Unregistered instance called cache-manager");
}

void TestIdxServer::force_stat_update() {

	bool all_idle = true;

	// Wait until connections are idle;
	do {
		all_idle = true;
		if ( !all_idle )
			std::this_thread::sleep_for( std::chrono::milliseconds(500) );

		for ( auto &kv : control_connections ) {
			all_idle &= kv.second->get_state() == ControlConnection::State::IDLE;
		}
	} while (!all_idle);

	for ( auto &kv : control_connections ) {
		kv.second->send_get_stats();
	}

	// Wait for finishing stat-update
	do {
		all_idle = true;
		if ( !all_idle )
			std::this_thread::sleep_for( std::chrono::milliseconds(500) );

		for ( auto &kv : control_connections ) {
			all_idle &= kv.second->get_state() == ControlConnection::State::IDLE;
		}
	} while (!all_idle);
}
