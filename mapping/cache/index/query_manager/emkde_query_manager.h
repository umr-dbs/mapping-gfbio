/*
 * bema_query_manager.h
 *
 *  Created on: 24.05.2016
 *      Author: koerberm
 */

#ifndef CACHE_INDEX_QUERY_MANAGER_EMKDE_QUERY_MANAGER_H_
#define CACHE_INDEX_QUERY_MANAGER_EMKDE_QUERY_MANAGER_H_

#include "cache/index/query_manager/simple_query_manager.h"
#include "util/gdal.h"

class EMNode {
public:
	EMNode( uint32_t id, uint32_t hilbert_bound = 0 ) : hilbert_bound(hilbert_bound), node_id(id) {};
	uint32_t hilbert_bound;
	uint32_t node_id;
};

class EMKDEQueryManager : public SimpleQueryManager {
public:
public:
	EMKDEQueryManager(const std::map<uint32_t,std::shared_ptr<Node>> &nodes, IndexCacheManager &mgr);
protected:
	std::unique_ptr<PendingQuery> create_job(const BaseRequest &req);

private:
	std::string bounds_to_string() const;

	uint32_t get_hilbert_value( const QueryRectangle &rect );

	void check_nodes_changed();
	double update_bins( uint32_t hv );
	void update_bounds(double fsum);

	std::vector<EMNode> bounds;
	std::array<double, 2000> bins;
	double alpha;
	uint32_t bandwith;

	static const uint32_t MAX_Z;
	static const uint32_t MASKS[];
	static const uint32_t SHIFTS[];
	static const uint16_t SCALE_X;
	static const uint16_t SCALE_Y;
	static const GDAL::CRSTransformer TRANS_GEOSMSG;
	static const GDAL::CRSTransformer TRANS_WEBMERCATOR;
};

#endif /* CACHE_INDEX_QUERY_MANAGER_EMKDE_QUERY_MANAGER_H_ */
