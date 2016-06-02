/*
 * querymanager.h
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#ifndef QUERYMANAGER_H_
#define QUERYMANAGER_H_

#include "cache/index/index_cache_manager.h"
#include "cache/priv/shared.h"
#include "cache/priv/connection.h"

#include <string>
#include <unordered_map>
#include <map>
#include <list>
#include <vector>
#include <set>

class QueryManager;



/**
 * Manages locks on cache-entries
 */
class CacheLocks {
public:
	/**
	 * Models a single lock
	 */
	class Lock : public IndexCacheKey {
	public:
		/**
		 * Creates a new instance for the given entry
		 * @param type the type of the cache entry
		 * @param key the entry's key
		 */
		Lock( CacheType type, const IndexCacheKey &key );
		bool operator<( const Lock &l ) const;
		bool operator==( const Lock &l ) const;
		CacheType type;
	};
public:
	/**
	 * @param type the type of the cache entry
	 * @param key the entry's key
	 * @return whether the given entry is locked
	 */
	bool is_locked( CacheType type, const IndexCacheKey &key ) const;
	/**
	 * @param lock the lock to check
	 * @return whether the given entry is locked
	 */
	bool is_locked( const Lock &lock ) const;

	/**
	 * Adds the given lock
	 * @param lock the lock to add
	 */
	void add_lock( const Lock &lock );

	/**
	 * Adds the given locks
	 * @param locks the locks to add
	 */
	void add_locks( const std::vector<Lock> &locks );

	/**
	 * Removes the given lock
	 * @param lock the lock to remove
	 */
	void remove_lock( const Lock &lock );

	/**
	 * Removes the given locks
	 * @param locks the locks to remove
	 */
	void remove_locks( const std::vector<Lock> &locks );
private:
	std::map<Lock,uint16_t> locks;
};

/**
 * Models a query currently executed by a worker
 */
class RunningQuery {
	friend class QueryManager;
	friend class IndexQueryStats;
public:
	/**
	 * Creates a new instance and sets the given locks
	 * @param locks the locks to obtain
	 */
	RunningQuery( std::vector<CacheLocks::Lock> &&locks = std::vector<CacheLocks::Lock>() );
	virtual ~RunningQuery();

	RunningQuery( const RunningQuery& ) = delete;
	RunningQuery( RunningQuery&& ) = delete;

	RunningQuery& operator=(const RunningQuery&) = delete;
	RunningQuery& operator=(RunningQuery&&) = delete;

	/**
	 * @return whether this query satisfies the given request
	 */
	bool satisfies( const BaseRequest &req ) const;

	/**
	 * Adds the given lock to the locks held by this query
	 * @param lock the lock to add
	 */
	void add_lock( const CacheLocks::Lock &key );

	/**
	 * Adds the given locks to the locks held by this query
	 * @param locks the locks to add
	 */
	void add_locks( const std::vector<CacheLocks::Lock> &locks );

	/**
	 * Adds the given client as a consumer of this query's result
	 * @param client the id of the client-connection to add
	 */
	void add_client( uint64_t client );

	/**
	 * Adds the given clients as a consumer of this query's result
	 * @param clients the ids of the client-connections to add
	 */
	void add_clients( const std::set<uint64_t> &clients );

	/**
	 * Removes the given client as a consumer of this query's result
	 * @param client the id of the client-connection to remove
	 */
	bool remove_client(uint64_t client_id);

	/**
	 * @return the ids of the client-connections, consuming this query's result
	 */
	const std::set<uint64_t>& get_clients() const;

	/**
	 * @return whether this query has consuming clients
	 */
	bool has_clients() const;

	/**
	 * @return the request used to schedule this query
	 */
	virtual const BaseRequest& get_request() const = 0;

private:
	std::vector<CacheLocks::Lock> locks;
	std::set<uint64_t> clients;
	uint64_t time_created;
	uint64_t time_scheduled;
	uint64_t time_finished;
};

/**
 * Models a query queued for execution
 */
class PendingQuery : public RunningQuery {
public:
	/**
	 * Creates a new instance and sets the given locks
	 * @param locks the locks to obtain
	 */
	PendingQuery( std::vector<CacheLocks::Lock> &&locks = std::vector<CacheLocks::Lock>() );
	virtual ~PendingQuery() = default;
	PendingQuery( const PendingQuery& ) = delete;
	PendingQuery( PendingQuery&& ) = default;

	PendingQuery& operator=(const PendingQuery&) = delete;
	PendingQuery& operator=(PendingQuery&&) = default;

	/**
	 * Extends this queries result dimension to satisfy the given request
	 * @param req the request to satisfy
	 * @return whether this query was extended
	 */
	virtual bool extend( const BaseRequest &req ) = 0;

	/**
	 * Schedules this query on one of the given connections, if possible
	 * @return the id of the worker-connection used for scheduling (0 if none).
	 */
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections ) = 0;

	/**
	 * @return whether this query depends on the node with the given id (e.g. references a cache-entry)
	 */
	virtual bool is_affected_by_node( uint32_t node_id ) = 0;
};

/**
 * Statistics about cache-queries on the index-server
 */
class IndexQueryStats : public QueryStats {
public:
	IndexQueryStats();

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	/**
	 * Resets this stats (setting all counts to 0)
	 */
	void reset();

	uint32_t get_queries_scheduled();
	void query_finished( const RunningQuery &q );
	void scheduled( uint32_t node_id );
	void issued();

private:
	uint32_t queries_issued;
	uint32_t queries_scheduled;
	std::map<uint32_t,uint64_t> node_to_queries;
	size_t num_queries;
	double avg_wait_time;
	double avg_exec_time;
	double avg_time;
};

/**
 * The query-manager manages all pending and running queries
 */
class QueryManager {
private:
	friend class RunningQuery;
	friend class CreateJob;
	static CacheLocks locks;
public:
	static std::unique_ptr<QueryManager> by_name( IndexCacheManager &mgr, const std::map<uint32_t,std::shared_ptr<Node>> &nodes, const std::string &name );

	virtual ~QueryManager() = default;

	/**
	 * Creates a new instance
	 * @param caches the available cache
	 * @param nodes a reference to the attached nodes
	 */
	QueryManager(const std::map<uint32_t,std::shared_ptr<Node>> &nodes, IndexCacheManager &caches);

	/**
	 * Adds a new client-request to the processing pipeline. The manager
	 * checks all running and pending queries, if their result might be
	 * used to answer the new query. If not a new job is queued for execution.
	 * @param client_id the connection-id of the client issued this query
	 * @param req the query-spec
	 */
	virtual void add_request( uint64_t client_id, const BaseRequest &req ) = 0;

	/**
	 * Processes cache-requests from workers.
	 * @param con the worker-connection issued the cache-query
	 */
	virtual void process_worker_query(WorkerConnection& con) = 0;

	/**
	 * Schedules the jobs waiting for exectuion, according to their
	 * preferred node.
	 * @param worker_connections the currently available worker-connections
	 */
	virtual void schedule_pending_jobs( const std::map<uint64_t, std::unique_ptr<WorkerConnection>> &worker_connections );

	/**
	 * Handle if a worker failed (e.g. the connection was lost).
	 * If a query was currently exectued, it is rescheduled on a different worker
	 * @param worker_id the id of the worker that failed
	 */
	void worker_failed( uint64_t worker_id );

	/**
	 * Handle if a whole node went away. All queries referencing
	 * entries on the node or should be scheduled on it, are
	 * re-analyzed and executed on a different node.
	 * @param node_id the id of the node that failed
	 */
	void node_failed( uint32_t node_id );

	/**
	 * Invoked after the computation of a result is finished.
	 * After this call no queries may be attached to this job.
	 * @param worker_id the id of the worker
	 */
	size_t close_worker( uint64_t worker_id );

	/**
	 * Releases the worker with the given id and returns the clients
	 * consuming the produced result. close_worker must have been invoked
	 * before calling this method
	 * @param worker_id the id of the worker
	 */
	std::set<uint64_t> release_worker( uint64_t worker_id );

	/**
	 * Handles cancelled client requests. If there are no other clients
	 * waiting for the result of the requested query, the query is cancelled.
	 * @param client_id the id of the disconnected client
	 */
	void handle_client_abort( uint64_t client_id );

	/**
	 * @return whether the given entry is locked by a query
	 */
	bool is_locked( CacheType type, const IndexCacheKey &key ) const;

	/**
	 * @return the query-statistics
	 */
	const IndexQueryStats& get_stats() const;

	/**
	 * Resets the query-statistics
	 */
	void reset_stats();
protected:
	/**
	 * Re-schedules a job after a worker-/node-failure
	 * @param query the query to reschedule
	 * @return a ready-to-schedule job satisfying the failed query
	 */
	virtual std::unique_ptr<PendingQuery> recreate_job( const RunningQuery &query ) = 0;

	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	IndexCacheManager &caches;
	std::unordered_map<uint64_t,std::unique_ptr<RunningQuery>> queries;
	std::unordered_map<uint64_t,std::unique_ptr<RunningQuery>> finished_queries;
	std::list<std::unique_ptr<PendingQuery>> pending_jobs;
	IndexQueryStats stats;
};

#endif /* QUERYMANAGER_H_ */
