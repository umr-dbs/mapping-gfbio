/*
 * connection.h
 *
 *  Created on: 23.06.2015
 *      Author: mika
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "util/binarystream.h"
#include "cache/cache.h"
#include "cache/priv/transfer.h"

#include <map>
#include <memory>

class Node;

class IndexConnection {
public:
	typedef std::unique_ptr<UnixSocket> SP;
	IndexConnection( SP &socket );
	virtual ~IndexConnection();
	int get_read_fd();
	const uint64_t id;
	virtual void input() = 0;
protected:
	BinaryStream &stream;
private:
	SP socket;
	static uint64_t next_id;
};

class ClientConnection : public IndexConnection {
public:
	enum class State { IDLE, REQUEST_READ, PROCESSING };
	enum class RequestType { NONE, RASTER, POINT, LINE, POLY, PLOT };

	static const uint32_t MAGIC_NUMBER = 0x22345678;
	ClientConnection( SP &socket );
	virtual ~ClientConnection();
	virtual void input();

	// Resets the state to REQUEST_READ after an
	// unexpected error in processing
	void retry();

	// Sets the state to PROCESSING
	// To be called after this connections
	// Job was assigned to a worker-instance
	void processing();

	State get_state() const;
	void send_response( const DeliveryResponse &response );
	void send_error( const std::string &message );

	RequestType get_request_type() const;
	const RasterBaseRequest& get_raster_request() const;
private:
	void reset();
	State state;
	RequestType request_type;
	std::unique_ptr<RasterBaseRequest> raster_request;
};

class WorkerConnection : public IndexConnection {
public:
	enum class State { IDLE, PROCESSING, DONE, ERROR };
	static const uint32_t MAGIC_NUMBER = 0x32345678;
	WorkerConnection( SP &socket, uint32_t node_id, RasterRefCache &raster_cache, const std::map<uint32_t,std::shared_ptr<Node>> &nodes );
	virtual ~WorkerConnection();
	virtual void input();

	State get_state() const;
	void process_request( uint64_t client_id, uint8_t command, const BaseRequest &request );
	void reset();

	uint64_t get_client_id() const;
	const DeliveryResponse &get_result() const;
	const std::string &get_error_message() const;

	const std::shared_ptr<Node> node;
private:
	void process_raster_request( const BaseRequest &req );

	RasterRefCache &raster_cache;
	const std::map<uint32_t,std::shared_ptr<Node>> &nodes;

	State state;
	uint64_t client_id;
	std::unique_ptr<DeliveryResponse> result;
	std::string error_msg;
};

class ControlConnection : public IndexConnection {
public:
	static const uint32_t MAGIC_NUMBER = 0x42345678;
	ControlConnection( SP &socket, const std::shared_ptr<Node> &node );
	virtual ~ControlConnection();
	virtual void input();

	const std::shared_ptr<Node> node;
};


#endif /* CONNECTION_H_ */
