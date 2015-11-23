/*
 * querymanager.h
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#ifndef QUERYMANAGER_H_
#define QUERYMANAGER_H_

#include "cache/index/index_cache.h"
#include "cache/priv/transfer.h"
#include "cache/priv/connection.h"
#include "cache/common.h"

#include <string>
#include <unordered_map>
#include <map>
#include <vector>

//
// Basic information about a query to run
// Holds all clients interessted in the results
// plus the basic data required to process the query
//

class QueryInfo : public BaseRequest {
public:
	QueryInfo( const BaseRequest &request );
	QueryInfo( CacheType type, const QueryRectangle &query, const std::string &semantic_id );
	bool satisfies( const BaseRequest &req ) const;
	void add_client( uint64_t client );
	void add_clients( const std::vector<uint64_t> &clients );
	const std::vector<uint64_t>& get_clients() const;
private:
	std::vector<uint64_t> clients;
};

//
// Describes a job to be scheduled
// Holds the corresponding Request which
// may be sent to a worker
//
class JobDescription : public QueryInfo {
public:
	virtual ~JobDescription() = default;
	JobDescription( const JobDescription& ) = default;
	JobDescription( JobDescription&& ) = default;

	JobDescription& operator=(JobDescription&&) = default;
	JobDescription& operator=(const JobDescription&) = default;

	// Extends this job by the given request
	// -> Enlarges the result to satisfy more than the original query
	virtual bool extend( const BaseRequest &req );
	// Schedules this job on one of the given worker-connections
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections ) = 0;
	virtual bool is_affected_by_node( uint32_t node_id ) = 0;
protected:
	JobDescription( std::unique_ptr<BaseRequest> request );
	std::unique_ptr<BaseRequest> request;

};

//
// Job describing the creation of a not yet cached result
//
class CreateJob : public JobDescription {
public:
	CreateJob( std::unique_ptr<BaseRequest> &request,
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const IndexCache &cache );
	bool extend( const BaseRequest &req );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
private:
	// The original query (before any calls to extends
	const QueryRectangle orig_query;
	const double orig_area;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	const IndexCache &cache;
};

//
// Job for delivering already cached results
//
class DeliverJob : public JobDescription {
public:
	DeliverJob( std::unique_ptr<DeliveryRequest> &request, uint32_t node );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
private:
	uint32_t node;
};

//
// Job for puzzling cached parts into a new result
//
class PuzzleJob : public JobDescription {
public:
	PuzzleJob( std::unique_ptr<PuzzleRequest> &request, std::vector<uint32_t> &nodes );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
private:
	std::vector<uint32_t> nodes;
};


//
// The query-manager
// Responsible for managing pending as well as already runnign jobs
//
class QueryManager {
public:
	QueryManager(IndexCaches &caches, const std::map<uint32_t,std::shared_ptr<Node>> &nodes);

	// Adds a new raster-request to the job queue
	// Might extend a pending request or consume the result
	// of an already running job.
	void add_request( uint64_t client_id, const BaseRequest &req );

	// Schedules pending jobs on the given worker-connections
	// It is not promised that all pending jobs are scheduled.
	// This depends on the available workers
	void schedule_pending_jobs( const std::map<uint64_t, std::unique_ptr<WorkerConnection>> &worker_connections );

	void worker_failed( uint64_t worker_id );

	void node_failed( uint32_t node_id );

	// closes this worker -- no requests will be accepted
	// Returns the number of clients waiting for its response
	size_t close_worker( uint64_t worker_id );

	// releases this worker -- returns the clients waiting for its response
	// worker MUST be closed before
	std::vector<uint64_t> release_worker( uint64_t worker_id );

private:
	std::unique_ptr<JobDescription> create_job( const BaseRequest &req );

	std::unique_ptr<JobDescription> recreate_job( const QueryInfo &query );

	IndexCaches &caches;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	std::unordered_map<uint64_t,QueryInfo> queries;
	std::unordered_map<uint64_t,QueryInfo> finished_queries;
	std::vector<std::unique_ptr<JobDescription>> pending_jobs;
};

#endif /* QUERYMANAGER_H_ */
