#ifndef OPERATORS_QUERYPROFILER_H
#define OPERATORS_QUERYPROFILER_H

#include <stdlib.h>

class QueryProfiler {
	public:
		QueryProfiler();
		QueryProfiler(const QueryProfiler &that) = delete;

		static double getTimestamp();

		double self_cpu;
		double all_cpu;
		double self_gpu;
		double all_gpu;
		size_t self_io;
		size_t all_io;
		// TODO: track GPU cost? Separately track things like Postgres queries?
		// TODO: track cached costs separately?

		void startTimer();
		void stopTimer();
		void addGPUCost(double seconds);
		void addIOCost(size_t bytes);

		QueryProfiler & operator+=(QueryProfiler &other);

	private:
		double t_start;
};

#endif
