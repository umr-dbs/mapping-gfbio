/*
 * node_config.cpp
 *
 *  Created on: 27.06.2016
 *      Author: koerberm
 */

#include "cache/node/node_config.h"
#include "util/configuration.h"

#include <sstream>

NodeConfig NodeConfig::fromConfiguration() {
	NodeConfig result;
	result.index_host = Configuration::get("indexserver.host");
	result.index_port = Configuration::getInt("indexserver.port");
	result.delivery_port = Configuration::getInt("nodeserver.port");
	result.num_workers = Configuration::getInt("nodeserver.threads",4);
	result.mgr_impl = Configuration::get("nodeserver.cache.manager");

	result.caching_strategy = Configuration::get("nodeserver.cache.strategy");
	result.mgr_impl = Configuration::get("nodeserver.cache.local.replacement", "lru");

	result.raster_size = Configuration::getInt("nodeserver.cache.raster.size");
	result.point_size = Configuration::getInt("nodeserver.cache.points.size");
	result.line_size = Configuration::getInt("nodeserver.cache.lines.size");
	result.polygon_size = Configuration::getInt("nodeserver.cache.polygons.size");
	result.plot_size = Configuration::getInt("nodeserver.cache.plots.size");
	return result;
}

NodeConfig::NodeConfig() :
		index_port(0),
		delivery_port(0),
		num_workers(1),
		raster_size(0),
		point_size(0),
		line_size(0),
		polygon_size(0),
		plot_size(0) {
}

std::string NodeConfig::to_string() const {
	std::ostringstream ss;
	ss << "NodeConfig:" << std::endl;
	ss << "  Index-Host       : " << index_host << std::endl;
	ss << "  Index-Port       : " << index_port << std::endl;
	ss << "  Delivery-Port    : " << delivery_port << std::endl;
	ss << "  #Workers         : " << num_workers << std::endl;
	ss << "  Manager-Impl     : " << mgr_impl << std::endl;
	ss << "  Caching-Strategy : " << caching_strategy << std::endl;
	ss << "  Local-Replacement: " << local_replacement << std::endl;
	ss << "  Raster-Size      : " << raster_size << std::endl;
	ss << "  Point-Size       : " << point_size << std::endl;
	ss << "  Line-Size        : " << line_size << std::endl;
	ss << "  Polygon-Size     : " << polygon_size << std::endl;
	ss << "  Plot-Size        : " << plot_size;
	return ss.str();
}
