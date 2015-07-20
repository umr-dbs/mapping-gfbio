/*
 * transfer.h
 *
 *  Created on: 11.06.2015
 *      Author: mika
 */

#ifndef TRANSFER_H_
#define TRANSFER_H_

//
// Holds helper types for transferring requests
// and responses between cache-client, index- and node-server
//

#include "operators/operator.h"
#include "util/binarystream.h"

#include <geos/geom/Geometry.h>
#include <string>
#include <sstream>
#include <memory>
#include <vector>

//
// Base class for a reference to something on a foreign node
//
class ForeignRef {
protected:
	ForeignRef( const std::string &host, uint32_t port );
	ForeignRef( BinaryStream &stream );

	ForeignRef( const ForeignRef &r ) = default;
	ForeignRef( ForeignRef &&r ) = default;
	virtual ~ForeignRef();

public:
	ForeignRef& operator=( const ForeignRef &r ) = default;
	ForeignRef& operator=( ForeignRef &&r ) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string()  const = 0;

	std::string host;
	uint32_t port;
};

//
// Reponse for the client
//
class DeliveryResponse : public ForeignRef {
public:
	DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id );
	DeliveryResponse( BinaryStream &stream );

	DeliveryResponse( const DeliveryResponse &r ) = default;
	DeliveryResponse( DeliveryResponse &&r ) = default;
	virtual ~DeliveryResponse();

	DeliveryResponse& operator=( const DeliveryResponse &r ) = default;
	DeliveryResponse& operator=( DeliveryResponse &&r ) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;

	uint64_t delivery_id;
};


//
// Reference to foreign cache-entries
// semantic_id must be retrieved from the context.
//
class CacheRef : public ForeignRef {
public:
	CacheRef( const std::string &host, uint32_t port, uint64_t entry_id );
	CacheRef( BinaryStream &stream );

	CacheRef( const CacheRef &r ) = default;
	CacheRef( CacheRef &&r ) = default;
	virtual ~CacheRef();

	CacheRef& operator=( const CacheRef &r ) = default;
	CacheRef& operator=( CacheRef &&r ) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;

	uint64_t entry_id;
};

//
// Basic request
// Used by the client to request something
// and also by the index to tell the node to
// produce somthing
//
class BaseRequest {
public:
	BaseRequest( const std::string &sem_id, const QueryRectangle &rect );
	BaseRequest( BinaryStream &stream );

	BaseRequest( const BaseRequest &r ) = default;
	BaseRequest( BaseRequest &&r ) = default;
	virtual ~BaseRequest();

	BaseRequest& operator=( const BaseRequest & r ) = default;
	BaseRequest& operator=( BaseRequest &&r ) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;

	std::string semantic_id;
	QueryRectangle query;
};

//
// Basic Delivery-Request
// Send by the index-server to the node in order
// to prepare a cached entry for delivery
//
class DeliveryRequest : public BaseRequest {
public:
	DeliveryRequest( const std::string &sem_id, const QueryRectangle &rect, uint64_t entry_id );
	DeliveryRequest( BinaryStream &stream );

	DeliveryRequest( const DeliveryRequest &r ) = default;
	DeliveryRequest( DeliveryRequest &&r ) = default;
	virtual ~DeliveryRequest();

	DeliveryRequest& operator=( const DeliveryRequest & r ) = default;
	DeliveryRequest& operator=( DeliveryRequest &&r ) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;

	uint64_t entry_id;
};

//
// Send from the index to the node to tell
// that a result should be combined from
// already cached results.
//
class PuzzleRequest : public BaseRequest {
public:
	typedef geos::geom::Geometry Geom;
	typedef std::unique_ptr<Geom> GeomP;

	PuzzleRequest( const std::string &sem_id, const QueryRectangle &rect, const GeomP &covered, const GeomP &remainder, const std::vector<CacheRef> &parts );
	PuzzleRequest( BinaryStream &stream );

	PuzzleRequest( const PuzzleRequest &r );
	PuzzleRequest( PuzzleRequest &&r );
	virtual ~PuzzleRequest();

	PuzzleRequest& operator=( const PuzzleRequest & r );
	PuzzleRequest& operator=( PuzzleRequest &&r );

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;
	virtual QueryRectangle get_remainder_query(double xres, double yres) const;

	GeomP covered;
	GeomP remainder;
	std::vector<CacheRef> parts;
};

#endif /* TRANSFER_H_ */
