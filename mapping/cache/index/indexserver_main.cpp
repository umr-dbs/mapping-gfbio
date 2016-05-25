/*
 * indexserver_main.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "cache/index/reorg_strategy.h"
#include "cache/common.h"
#include "util/configuration.h"
#include "util/log.h"
#include <signal.h>

IndexServer *instance = nullptr;

void termination_handler(int signum) {
	if (signum == SIGSEGV) {
		printf("Segmentation fault. Stacktrace:\n%s", CacheCommon::get_stacktrace().c_str());
		exit(1);
	}
	else {
		instance->stop();
	}
}

void set_signal_handler() {
	struct sigaction new_action, old_action;

	/* Set up the structure to specify the new action. */
	new_action.sa_handler = termination_handler;
	sigemptyset(&new_action.sa_mask);
	new_action.sa_flags = 0;

	sigaction(SIGINT, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGINT, &new_action, NULL);
	sigaction(SIGHUP, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGHUP, &new_action, NULL);
	sigaction(SIGTERM, NULL, &old_action);
	if (old_action.sa_handler != SIG_IGN)
		sigaction(SIGTERM, &new_action, NULL);
	sigaction(SIGSEGV, NULL, &old_action);
		sigaction(SIGSEGV, &new_action, NULL);
}

int main(void) {
	CacheCommon::set_uncaught_exception_handler();
	set_signal_handler();
	Configuration::loadFromDefaultPaths();

	// Disable GDAL Error Messages
	CPLSetErrorHandler(CacheCommon::GDALErrorHandler);

	auto portstr = Configuration::get("indexserver.port");

	Log::setLevel(Configuration::get("log.level","info"));

	auto portnr = atoi(portstr.c_str());

	std::string rs = Configuration::get("indexserver.reorg.strategy");
	std::string rel = Configuration::get("indexserver.reorg.relevance","lru");
	std::string scheduler = Configuration::get("indexserver.scheduler","default");

//	scheduler = "default";
	size_t update_interval = 2000;
	if ( scheduler == "dema" )
		update_interval = 0;

	instance = new IndexServer(portnr, update_interval, rs, rel, scheduler);
	instance->run();
	return 0;
}

