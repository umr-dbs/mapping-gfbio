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
#include <memory>

class QueryRectangle;
class GenericRaster;
class DeliveryConnection;

//
// Unique key generated for an entry in the cache
//
class STCacheKey {
public:
	STCacheKey( const std::string &semantic_id, uint64_t entry_id );
	STCacheKey( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	std::string semantic_id;
	uint64_t entry_id;
};

//
// Holds information about the spatial coverage of an entry
// for a query.
//
class STQueryInfo {
public:
	STQueryInfo( double coverage, double x1, double x2, double y1, double y2, uint64_t cache_id );
	bool operator <(const STQueryInfo &b) const;
	// coverage of issued query in [0,1]
	double coverage;
	// BBox of entry
	double x1, x2, y1, y2;
	// ID of entry
	uint64_t cache_id;
	// the score of the entry
	double get_score() const;
	std::string to_string() const;
};

//
// Describes the bounds of an cache-entry
//
class STEntryBounds : public SpatioTemporalReference {
public:
	STEntryBounds( epsg_t epsg,	double x1, double x2, double y1, double y2, double t1, double t2 );
	STEntryBounds( const SpatioTemporalReference &stref );
	STEntryBounds( BinaryStream &stream );

	STEntryBounds( const STEntryBounds &r ) = default;
	STEntryBounds( STEntryBounds &&r ) = default;
	virtual ~STEntryBounds() {};

	STEntryBounds& operator=( const STEntryBounds &r ) = default;
	STEntryBounds& operator=( STEntryBounds &&r ) = default;

	// Serializes the bounds
	virtual void toStream( BinaryStream &stream ) const;
	// Checks if the bounds fully cover the given query-rectangle
	virtual bool matches( const QueryRectangle &query ) const;
	// returns the spatial coverage if this entry for the given query-rectangle
	virtual double get_coverage( const QueryRectangle &query ) const;

	virtual std::string to_string() const;
};

//
// Describes the bounds of an raster-cache-entry.
// Additionally stores the resolution
//
class STRasterEntryBounds : public STEntryBounds {
public:
	STRasterEntryBounds( epsg_t epsg,	double x1, double x2, double y1, double y2, double t1, double t2, double x_res_from, double x_res_to, double y_res_from, double y_res_to );
	STRasterEntryBounds( const GenericRaster &raster );
	STRasterEntryBounds( BinaryStream &stream );

	STRasterEntryBounds( const STRasterEntryBounds &r ) = default;
	STRasterEntryBounds( STRasterEntryBounds &&r ) = default;
	virtual ~STRasterEntryBounds() {};

	STRasterEntryBounds& operator=( const STRasterEntryBounds &r ) = default;
	STRasterEntryBounds& operator=( STRasterEntryBounds &&r ) = default;

	// Serializes the bounds
	virtual void toStream( BinaryStream &stream ) const;
	// Checks if the bounds fully cover the given query-rectangle
	virtual bool matches( const QueryRectangle &query ) const;
	// returns the spatial coverage if this entry for the given query-rectangle
	virtual double get_coverage( const QueryRectangle &query ) const;

	virtual std::string to_string() const;

	double x_res_from, x_res_to, y_res_from, y_res_to;
};

//
// Reference to a cached raster
// We omit the semantic-id here, because it can be derived from the context
// The cache_id is the entry_id part of the corresponding STCacheKey
class STRasterRef {
public:
	STRasterRef( uint32_t node_id, uint64_t cache_id, const STRasterEntryBounds &bounds );
	const uint32_t node_id;
	const uint64_t cache_id;
	const STRasterEntryBounds bounds;
};

class STRasterRefKeyed : public STRasterRef {
public:
	STRasterRefKeyed( uint32_t node_id, const std::string &semantic_id, uint64_t cache_id, const STRasterEntryBounds &bounds );
	STRasterRefKeyed( uint32_t node_id, const STCacheKey &key, const STRasterEntryBounds &bounds );
	const std::string semantic_id;
};


class Delivery {
private:
	enum class Type { RASTER };
public:
	Delivery( uint64_t id, unsigned int count, std::unique_ptr<GenericRaster> &raster );

	Delivery( const Delivery &d ) = delete;
	Delivery( Delivery &&d );

	Delivery& operator=(const Delivery &d) = delete;
	Delivery& operator=(Delivery &&d) = delete;

	const uint64_t id;
	const time_t creation_time;
	unsigned int count;
	void send( DeliveryConnection &connection );
private:
	Type type;
	std::unique_ptr<GenericRaster> raster;
};



#endif /* TYPES_H_ */
