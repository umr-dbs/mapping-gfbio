#include "util/debug.h"
#include "raster/profiler.h"

#include <string>
#include <vector>

static std::vector<std::string> debugmessages;

void d(const std::string &str) {
	debugmessages.push_back(str);
}

std::vector<std::string> get_debug_messages() {
#if RASTER_DO_PROFILE
	auto all = debugmessages;
	auto profiler = Profiler::get();
	for (auto &str : profiler)
		all.push_back(str);
	return all;
#else
	return debugmessages;
#endif
}


void print_debug_header() {
	auto msgs = get_debug_messages();
	printf("Profiling-header: ");
	for (auto &str : msgs) {
		printf("%s, ", str.c_str());
	}
	printf("\r\n");
}
