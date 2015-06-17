/*
 * types.h
 *
 *  Created on: 08.06.2015
 *      Author: mika
 */

#ifndef TYPES_H_
#define TYPES_H_

#include "datatypes/spatiotemporal.h"
#include "util/binarystream.h"

class QueryRectangle;
class GenericRaster;

//
// Cube describing a cache-entry
// Used for querying the cache.
//
class CacheCube : public SpatioTemporalReference {
public:
	CacheCube( epsg_t epsg,	double x1, double x2, double y1, double y2, double t1, double t2 );
	CacheCube( const SpatioTemporalReference &stref );
	CacheCube( BinaryStream &stream );
	virtual ~CacheCube() {};
	virtual void toStream( BinaryStream &stream ) const;
	virtual bool matches( const QueryRectangle &query ) const;
};

//
// Cube for raster entries.
// Additionally holds the raster-resolution bounds.
//
class RasterCacheCube : public CacheCube {
public:
	RasterCacheCube( epsg_t epsg,	double x1, double x2, double y1, double y2, double t1, double t2, double x_res_from, double x_res_to, double y_res_from, double y_res_to );
	RasterCacheCube( const GenericRaster &raster );
	RasterCacheCube( BinaryStream &stream );
	virtual ~RasterCacheCube() {};
	virtual void toStream( BinaryStream &stream ) const;
	virtual bool matches( const QueryRectangle &query ) const;
private:
	double x_res_from, x_res_to, y_res_from, y_res_to;
};


//
// Reference to a cached raster
// We omit the semantic-id here, because it can be derived from the context
// The cache_id is the entry_id part of the corresponding STCacheKey
class RasterRef {
public:
	RasterRef( uint32_t node_id, uint64_t cache_id, const RasterCacheCube &cube );
	const uint32_t node_id;
	const uint64_t cache_id;
	const RasterCacheCube cube;
};


#endif /* TYPES_H_ */
