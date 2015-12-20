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
#include <set>

class CacheLocks {
public:
	class Lock : public IndexCacheKey {
	public:
		Lock( CacheType type, const IndexCacheKey &key );
		bool operator<( const Lock &l ) const;
		bool operator==( const Lock &l ) const;
		CacheType type;
	};
public:
	bool is_locked( CacheType type, const IndexCacheKey &key ) const;
	bool is_locked( const Lock &lock ) const;
	void add_lock( const Lock &lock );
	void add_locks( const std::vector<Lock> &locks );
	void remove_lock( const Lock &lock );
	void remove_locks( const std::vector<Lock> &locks );
private:
	std::map<Lock,uint16_t> locks;
};

//
// Basic information about a query to run
// Holds all clients interessted in the results
// plus the basic data required to process the query
//
class RunningQuery {
public:
	RunningQuery( std::vector<CacheLocks::Lock> &&locks = std::vector<CacheLocks::Lock>() );
	virtual ~RunningQuery();

	RunningQuery( const RunningQuery& ) = delete;
	RunningQuery( RunningQuery&& ) = delete;

	RunningQuery& operator=(const RunningQuery&) = delete;
	RunningQuery& operator=(RunningQuery&&) = delete;

	bool satisfies( const BaseRequest &req ) const;
	void add_lock( const CacheLocks::Lock &key );
	void add_locks( const std::vector<CacheLocks::Lock> &locks );

	void add_client( uint64_t client );
	void add_clients( const std::set<uint64_t> &clients );
	bool remove_client(uint64_t client_id);

	const std::set<uint64_t>& get_clients() const;
	bool has_clients() const;

	virtual const BaseRequest& get_request() const = 0;
private:
	std::vector<CacheLocks::Lock> locks;
	std::set<uint64_t> clients;
};

//
// Describes a job to be scheduled
// Holds the corresponding Request which
// may be sent to a worker
//
class PendingQuery : public RunningQuery {
public:
	PendingQuery( std::vector<CacheLocks::Lock> &&locks = std::vector<CacheLocks::Lock>() );
	virtual ~PendingQuery() = default;
	PendingQuery( const PendingQuery& ) = delete;
	PendingQuery( PendingQuery&& ) = default;

	PendingQuery& operator=(const PendingQuery&) = delete;
	PendingQuery& operator=(PendingQuery&&) = default;

	// Extends this job by the given request
	// -> Enlarges the result to satisfy more than the original query
	virtual bool extend( const BaseRequest &req ) = 0;
	// Schedules this job on one of the given worker-connections
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections ) = 0;
	virtual bool is_affected_by_node( uint32_t node_id ) = 0;
};

//
// Job describing the creation of a not yet cached result
//
class CreateJob : public PendingQuery {
public:
	CreateJob( BaseRequest &&request,
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const IndexCache &cache );
	bool extend( const BaseRequest &req );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
	const BaseRequest& get_request() const;
private:
	// The original query (before any calls to extends
	BaseRequest request;
	const QueryRectangle orig_query;
	const double orig_area;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	const IndexCache &cache;
};

//
// Job for delivering already cached results
//
class DeliverJob : public PendingQuery {
public:
	DeliverJob( DeliveryRequest &&request, const IndexCacheKey &key );
	bool extend( const BaseRequest &req );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
	const BaseRequest& get_request() const;
private:
	DeliveryRequest request;
	uint32_t node;
};

//
// Job for puzzling cached parts into a new result
//
class PuzzleJob : public PendingQuery {
public:
	PuzzleJob( PuzzleRequest &&request, const std::vector<IndexCacheKey> &keys );
	bool extend( const BaseRequest &req );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
	const BaseRequest& get_request() const;
private:
	PuzzleRequest request;
	std::set<uint32_t> nodes;
};


//
// The query-manager
// Responsible for managing pending as well as already runnign jobs
//
class QueryManager {
private:
	friend class RunningQuery;
	static CacheLocks locks;

public:
	QueryManager(IndexCaches &caches, const std::map<uint32_t,std::shared_ptr<Node>> &nodes);

	// Adds a new raster-request to the job queue
	// Might extend a pending request or consume the result
	// of an already running job.
	void add_request( uint64_t client_id, const BaseRequest &req );

	//
	// Handles worker-queries
	//
	void process_worker_query(WorkerConnection& con);

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
	std::set<uint64_t> release_worker( uint64_t worker_id );

	void handle_client_abort( uint64_t client_id );

	bool is_locked( CacheType type, const IndexCacheKey &key );

private:
	std::unique_ptr<PendingQuery> create_job( const BaseRequest &req );

	std::unique_ptr<PendingQuery> recreate_job( const RunningQuery &query );

	IndexCaches &caches;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	std::unordered_map<uint64_t,std::unique_ptr<RunningQuery>> queries;
	std::unordered_map<uint64_t,std::unique_ptr<RunningQuery>> finished_queries;
	std::vector<std::unique_ptr<PendingQuery>> pending_jobs;
};

#endif /* QUERYMANAGER_H_ */
