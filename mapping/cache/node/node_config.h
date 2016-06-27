/*
 * node_config.h
 *
 *  Created on: 27.06.2016
 *      Author: koerberm
 */

#ifndef CACHE_NODE_NODE_CONFIG_H_
#define CACHE_NODE_NODE_CONFIG_H_

#include <string>

class NodeConfig {
public:
	static NodeConfig fromConfiguration();

	NodeConfig();

	std::string index_host;
	int index_port;

	int delivery_port;
	int num_workers;


	std::string mgr_impl;
	size_t raster_size;
	size_t point_size;
	size_t line_size;
	size_t polygon_size;
	size_t plot_size;

	std::string caching_strategy;
	std::string local_replacement;

	std::string to_string() const;

};

#endif /* CACHE_NODE_NODE_CONFIG_H_ */
