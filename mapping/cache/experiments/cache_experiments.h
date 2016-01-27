/*
 * cache_experiment.h
 *
 *  Created on: 14.01.2016
 *      Author: mika
 */

#ifndef EXPERIMENTS_CACHE_EXPERIMENTS_H_
#define EXPERIMENTS_CACHE_EXPERIMENTS_H_

#include "cache/experiments/exp_util.h"

#include <iostream>
#include <chrono>

class LocalCacheExperiment : public CacheExperimentSingleQuery {
public:
	LocalCacheExperiment( const QuerySpec& spec, uint32_t num_runs, double p, uint32_t r );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	void execute( CacheManager *mgr, std::vector<size_t> &accum );
	double percentage; // used area
	uint32_t query_resolution;
	size_t capacity;
	std::vector<QTriple> queries;
	std::vector<size_t> uncached_accum;
	std::vector<size_t> cached_accum;
};


class PuzzleExperiment : public CacheExperimentSingleQuery {
public:
	PuzzleExperiment( const QuerySpec& spec, uint32_t num_runs, double p, uint32_t r );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	double percentage; // used area
	uint32_t query_resolution;
	size_t capacity;
	QueryRectangle query;
	double accum[4];
};

class StrategyExperiment : public CacheExperimentSingleQuery {
public:
	StrategyExperiment( const QuerySpec& spec, uint32_t num_runs, double p, uint32_t r );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	void exec( std::unique_ptr<CachingStrategy> strategy, std::pair<uint64_t,uint64_t> &accum );
	std::pair<uint64_t,uint64_t>& get_accum( const std::string &key );
	double percentage; // used area
	uint32_t query_resolution;
	size_t capacity;
	std::vector<QTriple> queries;
	std::map<std::string,std::pair<uint64_t,uint64_t>> accums;
};





class RelevanceExperiment : public CacheExperimentSingleQuery {
public:
	RelevanceExperiment( const QuerySpec& spec, uint32_t num_runs );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	std::vector<QTriple> generate_queries();
	void exec(const std::string& relevance, size_t capacity, double &accum);
	const std::vector<std::string> rels{"lru","costlru"};
	const std::vector<double> ratios{0.01,0.02,0.05,0.1,0.2};
	size_t capacity;
	std::vector<QTriple> queries;
	double accums[2][6];
};


class QueryBatchingExperiment : public CacheExperimentSingleQuery {
public:
	QueryBatchingExperiment( const QuerySpec& spec, uint32_t num_runs );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	size_t capacity;
	void exec( int nodes, int threads );
	std::vector<QTriple> queries;
	size_t queries_scheduled;
	QueryProfiler accum_unbatched;
	QueryProfiler accum_batched;

};


class ReorgExperiment : public CacheExperimentMultiQuery {
public:
	ReorgExperiment( const std::vector<QuerySpec>& specs, uint32_t num_runs );
protected:
	void global_setup();
	void setup();
	void print_results();
	void run_once();
private:
	void exec( const std::string &strategy, QueryStats &stats );
	size_t capacity;
	std::vector<QTriple> queries;
	QueryStats accum[3];
};

//class ReorgExperimentOld : public CacheExperimentSingleQuery {
//public:
//	ReorgExperimentOld( const QuerySpec& spec, uint32_t num_runs );
//protected:
//	void global_setup();
//	void setup();
//	void print_results();
//	void run_once();
//private:
//	void exec( const std::string &strategy, QueryStats &stats );
//	size_t capacity;
//	std::vector<QTriple> step1;
//	std::vector<QTriple> step2;
//	QueryStats accum[3];
//};

//class RelevanceExperimentOld : public CacheExperiment {
//public:
//	RelevanceExperimentOld( const QuerySpec& spec, uint32_t num_runs );
//protected:
//	void global_setup();
//	void setup();
//	void print_results();
//	void run_once();
//private:
//	const std::vector<std::string> rels{"lru","costlru"};
//	void exec( const std::string &relevance,size_t &accum );
//	std::vector<QTriple> queries;
//	size_t capacity;
//	size_t accums[2];
//};

#endif /* EXPERIMENTS_CACHE_EXPERIMENTS_H_ */
