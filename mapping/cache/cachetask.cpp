/*
 * cachetask.cpp
 *
 *  Created on: 17.05.2015
 *      Author: mika
 */

#include "cache/cachetask.h"
#include "operators/operator.h"

template<typename T>
CacheResponse<T>::~CacheResponse() {
}

template<typename T>
CacheResponse<T>::CacheResponse( std::unique_ptr<DataConverter<T> > converter, BinaryStream& stream) :
	message(), data(std::unique_ptr<T>(nullptr)), converter(std::move(converter)) {
	stream.read(&success);
	if (success) {
		data = this->converter->read(stream);
	}
	else {
		stream.read(&message);
	}
}

template<typename T>
void CacheResponse<T>::toStream(BinaryStream& stream) {
	stream.write(success);
	if (success)
		converter->write( stream, data );
	else
		stream.write(message);
}


std::unique_ptr<GenericRaster> RasterDataConverter::read(BinaryStream& stream) {
	return GenericRaster::fromStream(stream);
}

void RasterDataConverter::write(BinaryStream &stream, const std::unique_ptr<GenericRaster> &data) {
	data->toStream(stream);
}

RasterResponse::~RasterResponse() {
}

template class CacheResponse<GenericRaster>;

///////////////////////////////////////////////////////////////
//
// REQUESTS
//
///////////////////////////////////////////////////////////////


std::unique_ptr<CacheRequest> CacheRequest::fromStream(BinaryStream& stream) {
	RequestType rt;
	stream.read(&rt);
	switch (rt) {
	case CR_RASTER:
		return std::unique_ptr<CacheRequest>(new RasterRequest(stream));
	default:
		throw ArgumentException("Unknown request-type");
	}

}

CacheRequest::CacheRequest(RequestType type, BinaryStream& stream) :
		query(stream), type(type) {
	stream.read(&graphJson);
}

CacheRequest::~CacheRequest() {
}

void CacheRequest::toStream(BinaryStream& stream) {
	stream.write(type);
	query.toStream(stream);
	stream.write(graphJson);
}

RasterRequest::RasterRequest(BinaryStream& stream) :
		CacheRequest(CR_RASTER, stream) {
	int tmp;
	stream.read(&tmp);
	if (tmp == 0)
		qm = GenericOperator::RasterQM::LOOSE;
	else
		qm = GenericOperator::RasterQM::EXACT;
}

RasterRequest::~RasterRequest() {
}

void RasterRequest::toStream(BinaryStream &stream) {
	CacheRequest::toStream(stream);
	int tmp = qm == GenericOperator::RasterQM::LOOSE ? 0 : 1;
	stream.write(tmp);
}

void RasterRequest::execute(BinaryStream& stream) {
	auto graph = GenericOperator::fromJSON( graphJson );
	QueryProfiler qp;
	auto result = graph->getCachedRaster(query,qp,qm);
	RasterResponse rp(result);
	rp.toStream(stream);
}
