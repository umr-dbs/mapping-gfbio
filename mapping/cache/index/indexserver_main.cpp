/*
 * indexserver_main.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "util/configuration.h"
#include <signal.h>

IndexServer *instance = nullptr;

void termination_handler(int signum) {
	(void) signum;
	instance->stop();
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
}

int main(void) {
	set_signal_handler();
	Configuration::loadFromDefaultPaths();
	auto portstr = Configuration::get("indexserver.port");

	auto portnr = atoi(portstr.c_str());

	instance = new IndexServer(portnr);
	instance->run();
	return 0;
}

