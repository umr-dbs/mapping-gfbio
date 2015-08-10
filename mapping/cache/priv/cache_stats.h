/*
 * cache_stats.h
 *
 *  Created on: 06.08.2015
 *      Author: mika
 */

#ifndef CACHE_STATS_H_
#define CACHE_STATS_H_

#include "util/binarystream.h"

class Capacity {
public:
	Capacity( size_t raster_cache_size );
	Capacity( BinaryStream &stream );
	virtual ~Capacity();
	size_t get_raster_cache_total();
	size_t get_raster_cache_used();

	void add_raster( size_t size );
	void remove_raster( size_t size );
	double get_raster_usage();

	virtual void toStream( BinaryStream &stream );

	virtual std::string to_string();
private:
	size_t raster_cache_total;
	size_t raster_cache_used;
};

class OpStats {
public:
	OpStats();
	OpStats( BinaryStream &stream );

	void entry_added( uint64_t id, size_t size, double costs = 0 );
	void entry_accessed( uint64_t id );
	void entry_removed( uint64_t id );
};

class AccessInfo {
public:
	AccessInfo( uint64_t id );
	AccessInfo( BinaryStream &stream );

	void accessed();

	void toStream( BinaryStream &stream );

	uint64_t id;
	uint32_t count;
	time_t timestamp;
};

class NodeStats : public Capacity {
public:
	NodeStats( const Capacity &capacity );
	NodeStats( BinaryStream &stream );
	virtual ~NodeStats();
};

class NodeHandshake : public NodeStats {
public:
	NodeHandshake( const std::string &host, uint32_t port, const NodeStats &stats );
	NodeHandshake( BinaryStream &stream );
	virtual ~NodeHandshake();

	virtual void toStream( BinaryStream &stream );
	virtual std::string to_string();

	std::string host;
	uint32_t port;
};



#endif /* CACHE_STATS_H_ */
