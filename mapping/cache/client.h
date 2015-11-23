/*
 * cacheclient.h
 *
 *  Created on: 28.05.2015
 *      Author: mika
 */

#ifndef CLIENT_CLIENT_H_
#define CLIENT_CLIENT_H_

#include "cache/priv/transfer.h"
#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include <memory>

//
// A client used to communicate with the cache-index
//
class CacheClient {
public:
	CacheClient( std::string index_host, uint32_t index_port );
	virtual ~CacheClient();
	// Fetches the raster specified by the given query-parameters from the cache
	std::unique_ptr<GenericRaster> get_raster(	const std::string &graph_json,
												const QueryRectangle &query,
												const GenericOperator::RasterQM query_mode = GenericOperator::RasterQM::EXACT );

	std::unique_ptr<PointCollection> get_pointcollection(	const std::string &graph_json,
														const QueryRectangle &query,
														const GenericOperator::FeatureCollectionQM query_mode = GenericOperator::FeatureCollectionQM::ANY_FEATURE );

	std::unique_ptr<LineCollection> get_linecollection(	const std::string &graph_json,
														const QueryRectangle &query,
														const GenericOperator::FeatureCollectionQM query_mode = GenericOperator::FeatureCollectionQM::ANY_FEATURE );

	std::unique_ptr<PolygonCollection> get_polygoncollection(	const std::string &graph_json,
															const QueryRectangle &query,
															const GenericOperator::FeatureCollectionQM query_mode = GenericOperator::FeatureCollectionQM::ANY_FEATURE );

	std::unique_ptr<GenericPlot> get_plot(	const std::string &graph_json,
												const QueryRectangle &query );



private:
	template<typename T>
	std::unique_ptr<T> get_feature_collection(CacheType type,
										      const std::string &graph_json,
											  const QueryRectangle &query,
											  const GenericOperator::FeatureCollectionQM query_mode );

	std::unique_ptr<UnixSocket> process_request( CacheType type, const QueryRectangle &query, const std::string &workflow );
	std::string index_host;
	uint32_t index_port;
};


#endif /* CLIENT_CLIENT_H_ */
