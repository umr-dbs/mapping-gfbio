/*
 * shared.h
 *
 *  Created on: 02.03.2016
 *      Author: mika
 */

#ifndef PRIV_SHARED_H_
#define PRIV_SHARED_H_

#include "cache/priv/cube.h"
#include "operators/queryrectangle.h"
#include "operators/queryprofiler.h"
#include "datatypes/spatiotemporal.h"


#include "util/binarystream.h"

/**
 * Holds available types of computation resultss
 */
enum class CacheType : uint8_t { RASTER, POINT, LINE, POLYGON, PLOT, UNKNOWN };


/**
 * Stores information about the pixel-resolution of raster-data.
 * Especially the range for which a cached result is usable.
 */
class ResolutionInfo {
public:
	/**
	 * Constructs an empty info. Used for data without pixel-resolution
	 */
	ResolutionInfo();

	/**
	 * Constructs resolution information from the given result
	 * @param result the result to use
	 */
	ResolutionInfo( const GridSpatioTemporalResult &result );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	ResolutionInfo(BinaryReadBuffer &buffer);

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * Checks if the resolution matches the given query
	 * @param query The query to check the resolution for
	 * @return whether the resolution matches the query or not
	 */
	bool matches( const QueryRectangle &query ) const;

	QueryResolution::Type restype;
	Interval pixel_scale_x;
	Interval pixel_scale_y;
	double  actual_pixel_scale_x;
	double  actual_pixel_scale_y;
};


/**
 * Wraps a query-rectangle into a cube for searching the cache.
 */
class QueryCube : public Cube3 {
public:
	/**
	 * Constructs an instance from the given query
	 * @param query the query to wrap
	 */
	QueryCube( const QueryRectangle &rect );

	/**
	 * Constructs an instance from the given spatial and temporal references
	 * @param sref the spatial reference to use
	 * @param tref the temporal reference to use
	 */
	QueryCube( const SpatialReference &sref, const TemporalReference &tref );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	QueryCube( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	epsg_t epsg;
	timetype_t timetype;
};

/**
 * Describes the spatial, temporal and resolution bounds of a cache entry
 */
class CacheCube : public QueryCube {
public:
	/**
	 * Constructs an instance from the given spatial and temporal extension
	 * @param sref the spatial extension to use
	 * @param tref the temporal extension to use
	 */
	CacheCube( const SpatialReference &sref, const TemporalReference &tref );

	/**
	 * Constructs an instance from the given result
	 * @param result the result to use
	 */
	CacheCube( const SpatioTemporalResult &result );

	/**
	 * Constructs an instance from the given result
	 * @param result the result to use
	 */
	CacheCube( const GridSpatioTemporalResult & result );

	/**
	 * Constructs an instance ffrom the given plot
	 * @param result the plot to use
	 */
	CacheCube( const GenericPlot &result );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	CacheCube( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return the time-interval this cube is valid for
	 */
	const Interval& get_timespan() const;

	ResolutionInfo resolution_info;
};

/**
 * Basic information used when fetching an entry from another node
 */
class FetchInfo {
public:
	/**
	 * Constructs an instance from the given size and profile
	 * @param size the memory-size of the entry
	 * @param profile the cost-profile of the entry
	 */
	FetchInfo( uint64_t size, const ProfilingData &profile );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	FetchInfo( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	uint64_t size;
	ProfilingData profile;
};

/**
 * Holds all meta-information for a single cache-entry
 */
class CacheEntry : public FetchInfo {
public:
	/**
	 * Constructs an instance from the given bounds, size and profile
	 * @param bounds the cube describing the entry's bounds
	 * @param size the memory-size of the entry
	 * @param profile the cost-profile of the entry
	 */
	CacheEntry(CacheCube bounds, uint64_t size, const ProfilingData &profile);

	/**
	 * Constructs an instance from the given bounds, size, profile and access information
	 * @param bounds the cube describing the entry's bounds
	 * @param size the memory-size of the entry
	 * @param profile the cost-profile of the entry
	 * @param last_access The point in time the entry was accessed the last time
	 * @param access_count the number of accesses to the entry
	 */
	CacheEntry(CacheCube bounds, uint64_t size, const ProfilingData &profile, uint64_t last_access, uint32_t access_count);

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	CacheEntry( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	uint64_t last_access;
	uint32_t access_count;
	CacheCube bounds;
};

/**
 * A unique key for an entry in the local node cache
 */
class NodeCacheKey {
public:
	/**
	 * Constructs a new instance
	 * @param semantic_id the semantic id
	 * @param entry_id the entry's id
	 */
	NodeCacheKey( const std::string &semantic_id, uint64_t entry_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	NodeCacheKey( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	std::string semantic_id;
	uint64_t entry_id;
};

/**
 * A unique key for an entry in the local node cache, including the type.
 */
class TypedNodeCacheKey : public NodeCacheKey {
public:
	/**
	 * Constructs a new instance
	 * @param type the entry-type
	 * @param semantic_id the semantic id
	 * @param entry_id the entry's id
	 */
	TypedNodeCacheKey( CacheType type, const std::string &semantic_id, uint64_t entry_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	TypedNodeCacheKey( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	CacheType type;
};

/**
 * Holds all meta-information about a cache-entry in the local node-cache, as well
 * as the unique key to it.
 */
class MetaCacheEntry : public TypedNodeCacheKey, public CacheEntry {
public:
	/**
	 * Constructs a new instance from the given key and entry
	 * @param key the key
	 * @param entry the entry's meta-information
	 */
	MetaCacheEntry( const TypedNodeCacheKey &key, const CacheEntry &entry );

	/**
	 * Constructs a new instance from the given key and entry
	 * @param type the entry's type
	 * @param key the key
	 * @param entry the entry's meta-information
	 */
	MetaCacheEntry( CacheType type, const NodeCacheKey &key, const CacheEntry &entry );

	/**
	 * Constructs a new instance from the given key and entry
	 * @param type the entry-type
	 * @param semantic_id the semantic id
	 * @param entry_id the entry's id's type
	 * @param entry the entry's meta-information
	 */
	MetaCacheEntry( CacheType type, const std::string semantic_id, uint64_t entry_id, const CacheEntry &entry );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	MetaCacheEntry( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;
};



/**
 * Base class for referencing sth. on a foreign host
 */
class ForeignRef {
protected:
	/**
	 * Constructs a new instance
	 * @param host the name of the host
	 * @param port the port to connect to
	 */
	ForeignRef( const std::string &host, uint32_t port );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	ForeignRef( BinaryReadBuffer &buffer );

public:
	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	std::string host;
	uint32_t port;
};

/**
 * Models a response send to the client-stub.
 * Contains information about where to retrieve the
 * computation result from
 */
class DeliveryResponse : public ForeignRef {
public:
	/**
	 * Constructs a new instance
	 * @param host the name of the host
	 * @param port the port to connect to
	 * @param delivery_id the delivery id to request
	 */
	DeliveryResponse(std::string host, uint32_t port, uint64_t delivery_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	DeliveryResponse( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	uint64_t delivery_id;
};


/**
 * Models the reference to a cache-entry on a foreign node.
 */
class CacheRef : public ForeignRef {
public:
	/**
	 * Constructs a new instance
	 * @param host the name of the host
	 * @param port the port to connect to
	 * @param entry_id the unique id of the referenced entry
	 */
	CacheRef( const std::string &host, uint32_t port, uint64_t entry_id );

	/**
	 * Constructs an instance from the given buffer
	 * @param buffer The buffer holding the instance data
	 */
	CacheRef( BinaryReadBuffer &buffer );

	/**
	 * Serializes this instance to the given buffer
	 * @param buffer The buffer to write to
	 */
	void serialize(BinaryWriteBuffer &buffer, bool is_persistent_memory) const;

	/**
	 * @return a human readable respresentation
	 */
	std::string to_string() const;

	uint64_t entry_id;
};

#endif /* PRIV_SHARED_H_ */
