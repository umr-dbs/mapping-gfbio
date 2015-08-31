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

#include <string>
#include <unordered_map>
#include <map>
#include <vector>

//
// Basic information about a query to run
// Holds all clients interessted in the results
// plus the basic data required to process the query
//

class QueryInfo {
public:
	QueryInfo( const BaseRequest &request, uint64_t client );
	QueryInfo( const QueryRectangle &query, const std::string &semantic_id, uint64_t client );
	bool satisfies( const BaseRequest &req );
	void add_client( uint64_t client );
	const std::vector<uint64_t>& get_clients();
protected:
	QueryRectangle query;
	const std::string semantic_id;
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
	virtual ~JobDescription();
	// Extends this job by the given request
	// -> Enlarges the result to satisfy more than the original query
	virtual bool extend( const BaseRequest &req );
	// Schedules this job on one of the given worker-connections
	virtual uint64_t  schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections ) = 0;
protected:
	JobDescription( uint64_t client_id, std::unique_ptr<BaseRequest> request );
	std::unique_ptr<BaseRequest> request;

};

//
// Job describing the creation of a not yet cached result
//
class CreateJob : public JobDescription {
public:
	CreateJob( uint64_t client_id, std::unique_ptr<BaseRequest> &request );
	virtual ~CreateJob();
	virtual bool extend( const BaseRequest &req );
	virtual uint64_t  schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
private:
	// The original query (before any calls to extends
	const QueryRectangle orig_query;
	const double orig_area;

};

//
// Job for delivering already cached results
//
class DeliverJob : public JobDescription {
public:
	DeliverJob( uint64_t client_id, std::unique_ptr<DeliveryRequest> &request, uint32_t node );
	virtual ~DeliverJob();
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
private:
	uint32_t node;
};

//
// Job for puzzling cached parts into a new result
//
class PuzzleJob : public JobDescription {
public:
	PuzzleJob( uint64_t client_id, std::unique_ptr<PuzzleRequest> &request, std::vector<uint32_t> &nodes );
	virtual ~PuzzleJob();
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
private:
	std::vector<uint32_t> nodes;
};


//
// The query-manager
// Responsible for managing pending as well as already runnign jobs
//
class QueryManager {
public:
	QueryManager( const IndexCache &raster_cache, const std::map<uint32_t,std::shared_ptr<Node>> &nodes);
	virtual ~QueryManager();

	// Adds a new raster-request to the job queue
	// Might extend a pending request or consume the result
	// of an already running job.
	void add_raster_request( uint64_t client_id, const BaseRequest &req );

	// Schedules pending jobs on the given worker-connections
	// It is not promised that all pending jobs are scheduled.
	// This depends on the available workers
	void schedule_pending_jobs( const std::map<uint64_t, std::unique_ptr<WorkerConnection>> &worker_connections );

	// closes this worker -- no requests will be accepted
	// Returns the number of clients waiting for its response
	size_t close_worker( uint64_t worker_id );

	// releases this worker -- returns the clients waiting for its response
	// worker MUST be closed before
	std::vector<uint64_t> release_worker( uint64_t worker_id );

private:
	const IndexCache &raster_cache;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	std::unordered_map<uint64_t,QueryInfo> queries;
	std::unordered_map<uint64_t,QueryInfo> finished_queries;
	std::vector<std::unique_ptr<JobDescription>> pending_jobs;
};

#endif /* QUERYMANAGER_H_ */
