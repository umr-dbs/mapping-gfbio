
#include "operators/queryprofiler.h"
#include "util/exceptions.h"

#include <unistd.h>
#include <time.h>


/*
 * QueryProfiler class
 */
QueryProfiler::QueryProfiler() : self_cpu(0), all_cpu(0), self_gpu(0), all_gpu(0), self_io(0), all_io(0), t_start(0) {

}

double QueryProfiler::getTimestamp() {
#if defined(_POSIX_TIMERS) && defined(_POSIX_CPUTIME)
	struct timespec t;
	if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t) != 0)
		throw OperatorException("QueryProfiler: clock_gettime() failed");
	return (double) t.tv_sec + t.tv_nsec/1000000000.0;
#else
	#warning "QueryProfiler: Cannot query CPU time on this OS, using wall time instead"

	struct timeval t;
	if (gettimeofday(&t, nullptr) != 0)
		throw OperatorException("QueryProfiler: gettimeofday() failed");
	return (double) t.tv_sec + t.tv_usec/1000000.0;
#endif
}

void QueryProfiler::startTimer() {
	if (t_start != 0)
		throw OperatorException("QueryProfiler: Timer started twice");
	t_start = getTimestamp();
}

void QueryProfiler::stopTimer() {
	if (t_start == 0)
		throw OperatorException("QueryProfiler: Timer not started");
	double cost = getTimestamp() - t_start;
	t_start = 0;
	if (cost < 0)
		throw OperatorException("QueryProfiler: Timer stopped a negative time");
	self_cpu += cost;
	all_cpu += cost;
}

void QueryProfiler::addGPUCost(double seconds) {
	self_gpu += seconds;
	all_gpu += seconds;
}

void QueryProfiler::addIOCost(size_t bytes) {
	self_io += bytes;
	all_io += bytes;
}

QueryProfiler & QueryProfiler::operator+=(QueryProfiler &other) {
	if (other.t_start != 0)
		throw OperatorException("QueryProfiler: tried adding a timer that had not been stopped");
	all_cpu += other.all_cpu;
	all_gpu += other.all_gpu;
	all_io += other.all_io;
	return *this;
}
