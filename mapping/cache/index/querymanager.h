/*
 * querymanager.h
 *
 *  Created on: 10.07.2015
 *      Author: mika
 */

#ifndef QUERYMANAGER_H_
#define QUERYMANAGER_H_

#include "cache/priv/transfer.h"
#include "cache/priv/connection.h"

#include <string>
#include <unordered_map>
#include <map>
#include <vector>

//
// Jobs
//

class QueryInfo {
public:
	QueryInfo( const BaseRequest &request, uint64_t client );
	QueryInfo( const QueryRectangle &query, const std::string &semantic_id, uint64_t client );
	bool matches( const BaseRequest &req );
	void add_client( uint64_t client );
	const std::vector<uint64_t>& get_clients();
private:
	QueryRectangle query;
	std::string semantic_id;
	std::vector<uint64_t> clients;
};

class JobDescription : public QueryInfo {
public:
	virtual ~JobDescription();
	virtual uint64_t  schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections ) = 0;
protected:
	JobDescription( uint64_t client_id, std::unique_ptr<BaseRequest> request );
	const std::unique_ptr<BaseRequest> request;
};

class CreateJob : public JobDescription {
public:
	CreateJob( uint64_t client_id, std::unique_ptr<BaseRequest> &request );
	virtual ~CreateJob();
	virtual uint64_t  schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
};

class DeliverJob : public JobDescription {
public:
	DeliverJob( uint64_t client_id, std::unique_ptr<DeliveryRequest> &request, uint32_t node );
	virtual ~DeliverJob();
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
private:
	uint32_t node;
};

class PuzzleJob : public JobDescription {
public:
	PuzzleJob( uint64_t client_id, std::unique_ptr<PuzzleRequest> &request, std::vector<uint32_t> &nodes );
	virtual ~PuzzleJob();
	virtual uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
private:
	std::vector<uint32_t> nodes;
};

class QueryManager {
public:
	QueryManager( const RasterRefCache &raster_cache, const std::map<uint32_t,std::shared_ptr<Node>> &nodes);
	virtual ~QueryManager();

	void add_raster_request( uint64_t client_id, const BaseRequest &req );

	void schedule_pending_jobs( const std::map<uint64_t, std::unique_ptr<WorkerConnection>> &worker_connections );

	unsigned int get_query_count( uint64_t worker_id );

	std::vector<uint64_t> release_worker( uint64_t worker_id );

private:
	const RasterRefCache &raster_cache;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;
	std::unordered_map<uint64_t,QueryInfo> queries;
	std::vector<std::unique_ptr<JobDescription>> pending_jobs;
};

#endif /* QUERYMANAGER_H_ */
