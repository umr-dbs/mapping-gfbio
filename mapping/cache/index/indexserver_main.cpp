/*
 * indexserver_main.cpp
 *
 *  Created on: 26.05.2015
 *      Author: mika
 */

#include "cache/index/indexserver.h"
#include "cache/index/reorg_strategy.h"
#include "util/configuration.h"
#include <signal.h>
#include <execinfo.h>

IndexServer *instance = nullptr;

void termination_handler(int signum) {
	(void) signum;
	instance->stop();
}

void ex_handler() {
	void *trace_elems[20];
	int trace_elem_count(backtrace(trace_elems, 20));
	char **stack_syms(backtrace_symbols(trace_elems, trace_elem_count));
	for (int i = 0; i < trace_elem_count; ++i) {
		std::cout << stack_syms[i] << std::endl;
	}
	free(stack_syms);

	exit(1);
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
	std::set_terminate(ex_handler);
	set_signal_handler();
	Configuration::loadFromDefaultPaths();
	auto portstr = Configuration::get("indexserver.port");

	auto portnr = atoi(portstr.c_str());

	CapacityReorgStrategy cap_reorg;

	instance = new IndexServer(portnr, cap_reorg);
	instance->run();
	return 0;
}

