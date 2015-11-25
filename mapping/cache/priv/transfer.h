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

#include "cache/priv/cube.h"
#include "cache/common.h"
#include "operators/operator.h"
#include "util/binarystream.h"

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

	ForeignRef(const ForeignRef&) = default;
	ForeignRef(ForeignRef&&) = default;

	ForeignRef& operator=(ForeignRef&&) = default;
	ForeignRef& operator=(const ForeignRef&) = default;

public:
	virtual ~ForeignRef() = default;
	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const = 0;
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

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

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

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

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
	BaseRequest( CacheType type, const std::string &sem_id, const QueryRectangle &rect );
	BaseRequest( BinaryStream &stream );

	BaseRequest(const BaseRequest&) = default;
	BaseRequest(BaseRequest&&) = default;

	virtual ~BaseRequest() = default;

	BaseRequest& operator=(BaseRequest&&) = default;
	BaseRequest& operator=(const BaseRequest&) = default;

	virtual void toStream( BinaryStream &stream ) const;
	virtual std::string to_string() const;

	CacheType type;
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
	DeliveryRequest( CacheType type, const std::string &sem_id, const QueryRectangle &rect, uint64_t entry_id );
	DeliveryRequest( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	uint64_t entry_id;
};

//
// Send from the index to the node to tell
// that a result should be combined from
// already cached results.
//
class PuzzleRequest : public BaseRequest {
public:
	PuzzleRequest( CacheType type, const std::string &sem_id, const QueryRectangle &rect, const std::vector<Cube<3>> &remainder, const std::vector<CacheRef> &parts );
	PuzzleRequest( BinaryStream &stream );

	void toStream( BinaryStream &stream ) const;
	std::string to_string() const;

	std::vector<QueryRectangle> get_remainder_queries(double pixel_scale_x = 0, double pixel_scale_y = 0, double xref = 0, double yref = 0) const;
	std::vector<CacheRef> parts;
private:
	std::vector<Cube<3>>  remainder;
	void snap_to_pixel_grid( double &v1, double &v2, double ref, double scale ) const;
};

#endif /* TRANSFER_H_ */
