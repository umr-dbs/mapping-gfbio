/*
 * late_query_manager.h
 *
 *  Created on: 17.06.2016
 *      Author: koerberm
 */

#ifndef CACHE_INDEX_QUERY_MANAGER_LATE_QUERY_MANAGER_H_
#define CACHE_INDEX_QUERY_MANAGER_LATE_QUERY_MANAGER_H_

#include "cache/index/querymanager.h"

class LateJob : public PendingQuery {
public:
	LateJob( const BaseRequest &request, IndexCacheManager &caches );

	bool extend( const BaseRequest &req );
	bool is_affected_by_node( uint32_t node_id );
	uint64_t submit(const std::map<uint32_t, std::shared_ptr<Node>> &nmap);
	const BaseRequest& get_request() const;
protected:
	void replace_reference( const IndexCacheKey &from, const IndexCacheKey &to, const std::map<uint32_t, std::shared_ptr<Node>> &nmap );
private:
	IndexCacheManager &caches;
	BaseRequest request;
	const QueryRectangle orig_query;
	const double orig_area;
};


class LateQueryManager : public QueryManager {
public:
	LateQueryManager(const std::map<uint32_t,std::shared_ptr<Node>> &nodes,IndexCacheManager &caches);
	void add_request( uint64_t client_id, const BaseRequest &req );
	void process_worker_query(WorkerConnection& con);
	bool use_reorg() const;
protected:
	std::unique_ptr<PendingQuery> recreate_job( const RunningQuery &query );
private:
	IndexCacheManager &caches;
};

#endif /* CACHE_INDEX_QUERY_MANAGER_LATE_QUERY_MANAGER_H_ */
