/*
 * index_config.cpp
 *
 *  Created on: 27.06.2016
 *      Author: koerberm
 */

#include "index_config.h"
#include "util/configuration.h"

#include <sstream>

IndexConfig IndexConfig::fromConfiguration() {
	IndexConfig result;
	result.port = Configuration::getInt("indexserver.port");
	result.scheduler = Configuration::get("indexserver.scheduler","default");
	result.reorg_strategy = Configuration::get("indexserver.reorg.strategy");
	result.relevance_function = Configuration::get("indexserver.reorg.relevance","lru");
	result.update_interval = Configuration::getInt("indexserver.reorg.interval");
	result.batching_enabled = Configuration::getBool("indexserver.batching.enable",true);
	return result;
}

IndexConfig::IndexConfig() :
	port(0), update_interval(0), batching_enabled(true) {

}

std::string IndexConfig::to_string() const {
	std::ostringstream ss;
		ss << "IndexConfig:" << std::endl;
		ss << "  Port              : " << port << std::endl;
		ss << "  Scheduler         : " << scheduler << std::endl;
		ss << "  Reorg-Strategy    : " << reorg_strategy << std::endl;
		ss << "  Relevance-Function: " << relevance_function << std::endl;
		ss << "  Update-Interval   : " << update_interval << std::endl;
		ss << "  Batching          : " << batching_enabled;
		return ss.str();
}
