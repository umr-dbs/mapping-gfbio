/*
 * bema_query_manager.h
 *
 *  Created on: 24.05.2016
 *      Author: koerberm
 */

#ifndef CACHE_INDEX_QUERY_MANAGER_DEMA_QUERY_MANAGER_H_
#define CACHE_INDEX_QUERY_MANAGER_DEMA_QUERY_MANAGER_H_

#include "cache/index/querymanager.h"

/**
 * Describes a query where the whole result must be computed
 */
class DemaJob : public PendingQuery {
public:
	/**
	 * Creates a new instance
	 * @param request the request issued by the client
	 */
	DemaJob( const BaseRequest &request, uint32_t node_id );

	bool extend( const BaseRequest &req );
	uint64_t schedule( const std::map<uint64_t,std::unique_ptr<WorkerConnection>> &connections );
	bool is_affected_by_node( uint32_t node_id );
	const BaseRequest& get_request() const;
private:
	BaseRequest request;
	uint32_t node_id;
};

class ServerInfo {
public:
	ServerInfo(Point2 p) : p(p) {};
	Point<2> p;
};


class DemaQueryManager : public QueryManager {
public:
public:
	DemaQueryManager(const std::map<uint32_t,std::shared_ptr<Node>> &nodes);
	void add_request( uint64_t client_id, const BaseRequest &req );
	void process_worker_query(WorkerConnection& con);
protected:
	std::unique_ptr<PendingQuery> recreate_job( const RunningQuery &query );
private:
	std::unique_ptr<PendingQuery> create_job(const BaseRequest &req);

	double alpha;
	std::map<uint32_t,ServerInfo> infos;

};

#endif /* CACHE_INDEX_QUERY_MANAGER_DEMA_QUERY_MANAGER_H_ */
