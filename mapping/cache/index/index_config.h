/*
 * index_config.h
 *
 *  Created on: 27.06.2016
 *      Author: koerberm
 */

#ifndef CACHE_INDEX_INDEX_CONFIG_H_
#define CACHE_INDEX_INDEX_CONFIG_H_

#include <string>

class IndexConfig {
public:
	static IndexConfig fromConfiguration();
	IndexConfig();

	int port;
	std::string reorg_strategy;
	std::string relevance_function;
	std::string scheduler;
	int update_interval;
	bool batching_enabled;

	std::string to_string() const;
};

#endif /* CACHE_INDEX_INDEX_CONFIG_H_ */
