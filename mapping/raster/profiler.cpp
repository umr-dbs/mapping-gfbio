#include "raster/profiler.h"

#if RASTER_DO_PROFILE

#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <map>



namespace Profiler {
	static double getTimestamp() {
		struct timeval t;
		if (gettimeofday(&t, nullptr) != 0) {
			printf("gettimeofday() failed\n");
			exit(5);
		}
		return (double) t.tv_sec + t.tv_usec/1000000.0;
	}

	static std::vector< std::pair<std::string, double> > finished_timers;
	static std::map<std::string, double> running_timers;

	void start(const char *msg) {
		std::string key(msg);
		if (running_timers.count(key) > 0)
			return;

		running_timers.insert( std::make_pair(key, getTimestamp()) );
	}

	void stop(const char *msg) {
		std::string key(msg);
		if (running_timers.count(key) < 1)
			return;

		double starttime = running_timers.at(key);
		running_timers.erase(key);

		double elapsed_time = getTimestamp() - starttime;

		finished_timers.push_back( std::make_pair(key, elapsed_time) );
	}

	void print() {
		for (auto it = finished_timers.begin() ; it != finished_timers.end(); ++it) {
			printf("%s: %f, ", it->first.c_str(), it->second);
		}
	}
}


#endif