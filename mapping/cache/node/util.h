/*
 * util.h
 *
 *  Created on: 03.12.2015
 *      Author: mika
 */

#ifndef NODE_UTIL_H_
#define NODE_UTIL_H_

#include "util/binarystream.h"
#include "cache/priv/transfer.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/cache_structure.h"

#include <string>
#include <memory>

class NodeUtil {
public:
	NodeUtil();
	virtual ~NodeUtil() = default;

	virtual void set_self_port(uint32_t port);

	virtual void set_self_host( const std::string &host );

	virtual void set_index_connection( BinaryStream *con );

	virtual BinaryStream &get_index_connection();

	virtual CacheRef create_self_ref(uint64_t id) const;

	virtual bool is_self_ref(const CacheRef& ref) const;

	virtual NodeHandshake create_handshake() const;

	virtual NodeStats get_stats() const;

private:
	std::string my_host;
	uint32_t my_port;

// static
public:
	static NodeUtil& get_instance();
protected:
	static void set_instance( std::unique_ptr<NodeUtil> inst );
private:
	static thread_local BinaryStream *index_connection;
	static std::unique_ptr<NodeUtil> instance;
};



#endif /* NODE_UTIL_H_ */
