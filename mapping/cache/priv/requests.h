/*
 * requests.h
 *
 *  Created on: 02.03.2016
 *      Author: mika
 */

#ifndef PRIV_REQUESTS_H_
#define PRIV_REQUESTS_H_

#include "cache/priv/shared.h"
#include "util/binarystream.h"

/**
 * Basic Request.<br>
 * Used by the client-stub to request a computation result
 * and by the index to trigger the computation of a result on
 * a node.
 */
class BaseRequest {
public:
	/**
	 * Creates a new instance
	 * @param type the result-type
	 * @param semantic_id the semantic id
	 * @param query the query area
	 */
	BaseRequest( CacheType type, const std::string &semantic_id, const QueryRectangle &query );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	BaseRequest( BinaryReadBuffer &buffer );

	BaseRequest(const BaseRequest&) = default;
	BaseRequest(BaseRequest&&) = default;

	virtual ~BaseRequest() = default;

	BaseRequest& operator=(BaseRequest&&) = default;
	BaseRequest& operator=(const BaseRequest&) = default;

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	virtual void toStream( BinaryWriteBuffer &buffer ) const;

	/**
	 * @return a human readable respresentation
	 */
	virtual std::string to_string() const;

	CacheType type;
	std::string semantic_id;
	QueryRectangle query;
};

/**
 * Request issued by the index-server to deliver
 * a cached-entry without the need of any computation
 */
class DeliveryRequest : public BaseRequest {
public:
	/**
	 * Creates a new instance
	 * @param type the result-type
	 * @param semantic_id the semantic id
	 * @param query the query area
	 * @param entry_id the id of the entry to deliver
	 */
	DeliveryRequest( CacheType type, const std::string &semantic_id, const QueryRectangle &query, uint64_t entry_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	DeliveryRequest( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void toStream( BinaryWriteBuffer &buffer ) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	uint64_t entry_id;
};

/**
 * Request issued by the index-server to construct a
 * result by combining one or more cache-entrys and
 * remainders which need to be computed.
 */
class PuzzleRequest : public BaseRequest {
public:

	/**
	 * Creates a new instance
	 * @param type the result-type
	 * @param semantic_id the semantic id
	 * @param query the query area
	 * @param remainder the list of remaining computations
	 * @param parts the list of cache-entries required
	 */
	PuzzleRequest( CacheType type, const std::string &semantic_id, const QueryRectangle &query, const std::vector<Cube<3>> &remainder, const std::vector<CacheRef> &parts );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	PuzzleRequest( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void toStream( BinaryWriteBuffer &buffer ) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	/**
	 * @return the number of remainders
	 */
	size_t get_num_remainders() const;

	/**
	 * @return whether remainders need to be computed or not
	 */
	bool has_remainders() const;

	/**
	 * Transforms the remainder-cubes into queries understood by mapping
	 * @param ref_result a reference-result
	 */
	template<class T>
	std::vector<QueryRectangle> get_remainder_queries(const T& ref_result) const;
	std::vector<CacheRef> parts;
private:
	std::vector<Cube<3>>  remainder;
	/**
	 * Snaps the bounds of a cube to the pixel grid
	 * @param v1 the first value of the interval
	 * @param v2 the second value of the interval
	 * @param ref the reference point (value where a full pixel starts)
	 * @param scale the pixel-scale
	 *
	 */
	void snap_to_pixel_grid( double &v1, double &v2, double ref, double scale ) const;
};

#endif /* PRIV_REQUESTS_H_ */
