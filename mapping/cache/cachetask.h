/*
 * cachetask.h
 *
 *  Created on: 17.05.2015
 *      Author: mika
 */

#ifndef CACHETASK_H_
#define CACHETASK_H_

#include <memory>
#include "operators/operator.h"
#include "util/binarystream.h"

enum RequestType : uint16_t {
	CR_RASTER ,
	CR_POINTCOLLECTION,
	CR_LINECOLLECTION,
	CR_POLYGONCOLLECTION,
	CR_PLOT
};

template<typename T>
class DataConverter {
public:
	virtual ~DataConverter() {};
	virtual std::unique_ptr<T> read( BinaryStream &stream ) = 0;
	virtual void write( BinaryStream &stream, const std::unique_ptr<T> &data ) = 0;
};

template<typename T>
class CacheResponse {
protected:
	CacheResponse( std::unique_ptr<DataConverter<T>> converter, const std::string &message ) :
		success(false), message(message), data(std::unique_ptr<T>(nullptr)), converter(std::move(converter) ) {};
	CacheResponse( std::unique_ptr<DataConverter<T>> converter, std::unique_ptr<T> &data ) :
			success(true), message(), data(std::move(data)), converter(std::move(converter)) {};
	CacheResponse( std::unique_ptr<DataConverter<T>> converter, BinaryStream &stream );
public:
	virtual ~CacheResponse();
	void toStream( BinaryStream &stream );
	bool success;
	std::string message;
	std::unique_ptr<T> data;
private:
	std::unique_ptr<DataConverter<T>> converter;
};


class RasterDataConverter : public DataConverter<GenericRaster> {
public:
	virtual ~RasterDataConverter() {};
	virtual std::unique_ptr<GenericRaster> read( BinaryStream &stream );
	virtual void write( BinaryStream &stream, const std::unique_ptr<GenericRaster> &data );
};

class RasterResponse : public CacheResponse<GenericRaster> {
public:
	virtual ~RasterResponse();
	RasterResponse( const std::string &message ) : CacheResponse( std::unique_ptr<DataConverter<GenericRaster>>(new RasterDataConverter() ) ,message) {};
	RasterResponse( std::unique_ptr<GenericRaster> &data ) : CacheResponse(std::unique_ptr<DataConverter<GenericRaster>>(new RasterDataConverter() ), data) {};
	RasterResponse( BinaryStream &stream ) : CacheResponse(std::unique_ptr<DataConverter<GenericRaster>>(new RasterDataConverter() ), stream) {};
};


///////////////////////////////////////////////////////////////
//
// REQUESTS
//
///////////////////////////////////////////////////////////////

class CacheRequest {
protected:
	CacheRequest( RequestType type, const std::string &graphJson, const QueryRectangle& query ) :
		graphJson(graphJson), query(query), type(type)  {};
	CacheRequest( RequestType type, BinaryStream &stream );
public:
	static std::unique_ptr<CacheRequest> fromStream( BinaryStream &stream );
	virtual ~CacheRequest();
	virtual void toStream( BinaryStream &stream );
	virtual void execute( BinaryStream &stream ) = 0;
	std::string graphJson;
	QueryRectangle query;
private:
	const RequestType type;
};

class RasterRequest : public CacheRequest {
	friend class CacheRequest;
protected:
	RasterRequest( BinaryStream &stream );
public:
	virtual ~RasterRequest();
	RasterRequest( std::string graphJson, QueryRectangle query, GenericOperator::RasterQM qm ) : CacheRequest(CR_RASTER, graphJson, query), qm(qm) {};
	virtual void toStream( BinaryStream &stream );
	virtual void execute( BinaryStream &stream );
	GenericOperator::RasterQM qm;
};

#endif /* CACHETASK_H_ */
