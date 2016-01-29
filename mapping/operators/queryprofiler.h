#ifndef OPERATORS_QUERYPROFILER_H
#define OPERATORS_QUERYPROFILER_H

#include "util/binarystream.h"

#include <stdlib.h>
#include <string>

class ProfilingData {
public:
	ProfilingData();
	ProfilingData( BinaryStream &stream );
	void toStream(BinaryStream &stream) const;
	std::string to_string() const;
	double self_cpu;
	double all_cpu;
	double uncached_cpu;
	double self_gpu;
	double all_gpu;
	double uncached_gpu;
	uint64_t self_io;
	uint64_t all_io;
	uint64_t uncached_io;
};

class QueryProfiler : public ProfilingData {
	public:
		QueryProfiler();
		QueryProfiler(const QueryProfiler &that) = delete;

		static double getTimestamp();

		// TODO: track GPU cost? Separately track things like Postgres queries?
		// TODO: track cached costs separately?

		void startTimer();
		void stopTimer();
		void addGPUCost(double seconds);
		void addIOCost(size_t bytes);


		QueryProfiler & operator+=( const ProfilingData &other );
		QueryProfiler & operator+=( const QueryProfiler &other );
		void addTotalCosts( const ProfilingData &profile );
		void cached( const ProfilingData &profile );

	private:
		double t_start;
};

// these are three RAII helper classes to make sure that profiling works even when an operator throws an exception

class QueryProfilerSimpleGuard {
public:
	QueryProfilerSimpleGuard( QueryProfiler &profiler ) : profiler(profiler) {
		profiler.startTimer();
	};

	~QueryProfilerSimpleGuard() {
		profiler.stopTimer();
	}

	QueryProfiler &profiler;
};

class QueryProfilerRunningGuard {
	public:
		QueryProfilerRunningGuard(QueryProfiler &parent_profiler, QueryProfiler &profiler)
			: parent_profiler(parent_profiler), profiler(profiler) {
			profiler.startTimer();
		}
		~QueryProfilerRunningGuard() {
			profiler.stopTimer();
			parent_profiler += profiler;

		}
		QueryProfiler &parent_profiler;
		QueryProfiler &profiler;
};

class QueryProfilerStoppingGuard {
	public:
		QueryProfilerStoppingGuard(QueryProfiler &profiler) : profiler(profiler) {
			profiler.stopTimer();
		}
		~QueryProfilerStoppingGuard() {
			profiler.startTimer();
		}
		QueryProfiler &profiler;
};

#endif
