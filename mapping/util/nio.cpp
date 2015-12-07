/*
 * nio.cpp
 *
 *  Created on: 13.08.2015
 *      Author: mika
 */

#include "util/nio.h"
#include "util/exceptions.h"
#include "util/concat.h"
#include "util/make_unique.h"
#include "util/log.h"
#include "cache/priv/cache_structure.h"
#include "cache/priv/transfer.h"
#include "cache/priv/redistribution.h"
#include "cache/priv/cache_stats.h"
#include "cache/priv/connection.h"
#include "cache/common.h"

#include <errno.h>
#include <unistd.h>

//
// Simple helper
//


void StreamBuffer::write(const char* buffer, size_t len) {
	stream.write(buffer,len);
}

size_t StreamBuffer::read(char* buffer, size_t len, bool allow_eof) {

	(void) allow_eof;
	//Log::error("StreamSize: %d", stream.str().length());
	stream.read(buffer, len);
	if ( stream.bad() || stream.eof() || stream.fail() )
		throw ArgumentException( "Unexpected error on stream" );
	return len;
}

void StreamBuffer::reset() {
	stream.str("");
}

std::string StreamBuffer::get_content() const {
	return stream.str();
}

//
// Writer interface
//

NBWriter::NBWriter() {
}

NBWriter::~NBWriter() {
}

//
// Simple Writer
//

template<typename T>
NBSimpleWriter<T>::NBSimpleWriter(const T& value, bool use_dynamic_type) :
	bytes_written(0), total_bytes(0), error(false), use_dynamic_type(use_dynamic_type), data(nullptr){
	set_data(value);
}

template<typename T>
void NBSimpleWriter<T>::set_data( const T& value ) {
	set<T>(value);
	bytes_written = 0;
	error = false;
}

template<typename T>
NBSimpleWriter<T>::~NBSimpleWriter() {
	if ( data != nullptr )
		free(data);
}

template<typename T>
void NBSimpleWriter<T>::write(int fd) {
	if ( error || is_finished() )
			throw IllegalStateException(concat("Illegal state for writing. Error: ", error, ", Finished: ", is_finished()));

	ssize_t currently_written = 0;
	while ( currently_written >= 0 && !is_finished() ) {
		currently_written = ::write(fd,data+bytes_written,total_bytes-bytes_written);
		if ( currently_written >= 0 )
			bytes_written += currently_written;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", bytes_written, strerror(errno));
			error = true;
			return;
		}
	}
}

template<typename T>
bool NBSimpleWriter<T>::has_error() const {
	return error;
}

template<typename T>
bool NBSimpleWriter<T>::is_finished() const {
	return bytes_written >= total_bytes;
}

template<typename T>
size_t NBSimpleWriter<T>::get_total_written() const {
	return bytes_written;
}

template<typename T>
std::string NBSimpleWriter<T>::to_string() const {
	return concat("SimpleNBWriter[ written: ", bytes_written, ", finished: ", is_finished(), ", error: ", error ,"]");
}

//
// Container Writer
//
template<typename CType, typename ElementWriter>
NBContainerWriter<CType,ElementWriter>::NBContainerWriter( const CType &container, bool write_size) :
	element_count(container.size()),
	write_size(write_size),
	total_written(0),
	error(false),
	iter(container.begin()),
	end(container.end() ){

	if ( container.size() > 0 )
		e_writer.reset( new ElementWriter(*iter) );
}


template<typename CType, typename ElementWriter>
void NBContainerWriter<CType,ElementWriter>::write(int fd) {
	if ( error || is_finished() )
		throw IllegalStateException(concat("Illegal state for writing. Error: ", error, ", Finished: ", is_finished()));

	// Write size if required
	if ( write_size && total_written < sizeof(element_count) ) {
		unsigned char * data = (unsigned char*) &element_count;
		ssize_t currently_written = 0;
		while ( currently_written >= 0 && total_written < sizeof(element_count) ) {
			currently_written = ::write(fd,data+total_written,sizeof(element_count)-total_written);
			if ( currently_written >= 0 )
				total_written += currently_written;
			else if ( errno == EAGAIN || errno == EWOULDBLOCK )
				return;
			else {
				Log::debug("ERROR after %d bytes: %s", total_written, strerror(errno));
				error = true;
				return;
			}
		}
	}

	while ( iter != end ) {
		e_writer->write(fd);
		if ( e_writer->is_finished() ) {
			iter++;
			if ( iter != end )
				e_writer->set_data(*iter);
		}
		else if ( e_writer.has_error() )
			error = true;
		else
			break;
	}
}

template<typename CType, typename ElementWriter>
bool NBContainerWriter<CType,ElementWriter>::has_error() const {
	return error;
}

template<typename CType, typename ElementWriter>
bool NBContainerWriter<CType,ElementWriter>::is_finished() const {
	return iter == end;
}

template<typename CType, typename ElementWriter>
size_t NBContainerWriter<CType,ElementWriter>::get_total_written() const {
	return total_written;
}

template<typename CType, typename ElementWriter>
std::string NBContainerWriter<CType,ElementWriter>::to_string() const {
	return concat("NBContainerWriter[ written: ", get_total_written(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}

//
// PairWriter
//
template<typename K, typename V, typename KWriter, typename VWriter>
NBPairWriter<K, V, KWriter, VWriter>::NBPairWriter( const std::pair<K,V> &p ) {
	kw->reset( new KWriter(p.first) );
	vw->reset( new KWriter(p.second) );
}

template<typename K, typename V, typename KWriter, typename VWriter>
void NBPairWriter<K, V, KWriter, VWriter>::write(int fd) {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for writing. Error: ", has_error(), ", Finished: ", is_finished()));

	if ( !kw->is_finished() ) {
		kw->write(fd);
		if ( !kw->is_finished() )
			return;
	}

	vw->write(fd);

}

template<typename K, typename V, typename KWriter, typename VWriter>
bool NBPairWriter<K, V, KWriter, VWriter>::has_error() const {
	return kw->has_error() || vw->has_error();
}

template<typename K, typename V, typename KWriter, typename VWriter>
bool NBPairWriter<K, V, KWriter, VWriter>::is_finished() const {
	return kw->is_finished() && vw->is_finished();
}

template<typename K, typename V, typename KWriter, typename VWriter>
size_t NBPairWriter<K, V, KWriter, VWriter>::get_total_written() const {
	return kw->get_total_written() && vw->get_total_written();
}

template<typename K, typename V, typename KWriter, typename VWriter>
std::string NBPairWriter<K, V, KWriter, VWriter>::to_string() const {
	return "PairWriter";
}

template<typename K, typename V, typename KWriter, typename VWriter>
void NBPairWriter<K, V, KWriter, VWriter>::set_data(const std::pair<K, V>& data) {
	kw->set_data(data.first);
	vw->set_data(data.second);
}


//
// Multi Writer
//
NBMultiWriter::NBMultiWriter( std::unique_ptr<NBWriter> w1, std::unique_ptr<NBWriter> w2 ) : current_index(0) {
	add_writer( std::move(w1) );
	add_writer( std::move(w2) );
}

NBMultiWriter::NBMultiWriter(std::unique_ptr<NBWriter> w1,
		std::unique_ptr<NBWriter> w2, std::unique_ptr<NBWriter> w3) : current_index(0) {
	add_writer( std::move(w1) );
	add_writer( std::move(w2) );
	add_writer( std::move(w3) );
}

NBMultiWriter::NBMultiWriter(std::vector<std::unique_ptr<NBWriter> > writers) :
	current_index(0), writers(std::move(writers)) {
	for ( auto &w : this->writers ) {
		check_writer(*w);
	}
}

NBMultiWriter::NBMultiWriter() :
	current_index(0) {
}

void NBMultiWriter::add_writer(std::unique_ptr<NBWriter> w) {
	check_writer(*w);
	writers.push_back( std::move(w) );
}

void NBMultiWriter::check_writer(const NBWriter& w) {
	if ( get_total_written() > 0 || (writers.size() > 0 && (has_error() || is_finished())) )
		throw IllegalStateException("Can only add writer on fresh instance.");
	if ( w.get_total_written() > 0 ||
		 w.has_error() || w.is_finished() )
		throw ArgumentException("Can only build multi-writer with fresh writers.");
}

void NBMultiWriter::write(int fd) {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for writing. Error: ", has_error(), ", Finished: ", is_finished()));

	while ( current_index < writers.size()  ) {
		auto &w = writers.at(current_index);
		w->write(fd);
		if ( w->is_finished() )
			current_index++;
		else
			break;
	}
}

bool NBMultiWriter::has_error() const {
	return current_index < writers.size() && writers.at(current_index)->has_error();
}

bool NBMultiWriter::is_finished() const {
	return current_index == writers.size();
}

size_t NBMultiWriter::get_total_written() const {
	ssize_t res(0);
	for ( auto &w : writers ) {
		res += w->get_total_written();
	}
	return res;
}

std::string NBMultiWriter::to_string() const {
	return concat("MultiNBWriter[ #writer: ", writers.size(), ", written: ", get_total_written(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}

//
// Raster-Writer
//

class NBRasterDataWriter : public NBWriter {
public:
	NBRasterDataWriter( std::shared_ptr<const GenericRaster> raster);
	void write(int fd);
	bool has_error() const;
	bool is_finished() const;
	size_t get_total_written() const;
	std::string to_string() const;
private:
	std::shared_ptr<const GenericRaster> raster;
	size_t bytes_written;
	bool error;
};

NBRasterDataWriter::NBRasterDataWriter( std::shared_ptr<const GenericRaster>  raster) :
  raster(raster), bytes_written(0), error(false) {
}

void NBRasterDataWriter::write(int fd) {
	if ( has_error() || is_finished() )
			throw IllegalStateException(concat("Illegal state for writing. Error: ", has_error(), ", Finished: ", is_finished()));

	const char *data = (const char*)const_cast<GenericRaster*>(raster.get())->getData();

	ssize_t currently_written = 0;
	while ( currently_written >= 0 && !is_finished() ) {
		currently_written = ::write(fd,data+bytes_written,raster->getDataSize()-bytes_written);
		if ( currently_written >= 0 )
			bytes_written += currently_written;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", bytes_written, strerror(errno));
			error = true;
			return;
		}
	}
}

bool NBRasterDataWriter::has_error() const {
	return error;
}

bool NBRasterDataWriter::is_finished() const {
	return bytes_written >= raster->getDataSize();
}

size_t NBRasterDataWriter::get_total_written() const {
	return bytes_written;
}

std::string NBRasterDataWriter::to_string() const {
	return "RasterDataWriter";
}


NBRasterWriter::NBRasterWriter( std::shared_ptr<const GenericRaster> raster ) {
	add_writer(make_unique<NBSimpleWriter<DataDescription>>( raster->dd ));
	add_writer(make_unique<NBSimpleWriter<SpatioTemporalReference>>( raster->stref ));
	add_writer(make_unique<NBSimpleWriter<uint32_t>>( raster->width ));
	add_writer(make_unique<NBSimpleWriter<uint32_t>>( raster->height ));
	add_writer(make_unique<NBRasterDataWriter>( raster ));
	add_writer(make_unique<NBSimpleWriter<AttributeMaps>>( raster->global_attributes ));
}



NBPointsWriter::NBPointsWriter( std::shared_ptr<const PointCollection> points ) :
	NBSimpleWriter( *points ) {
}

NBLinesWriter::NBLinesWriter(std::shared_ptr<const LineCollection> lines) :
	NBSimpleWriter(*lines) {
}

NBPolygonsWriter::NBPolygonsWriter(std::shared_ptr<const PolygonCollection> polys) :
	NBSimpleWriter(*polys) {
}

NBPlotWriter::NBPlotWriter(std::shared_ptr<const GenericPlot> plot) :
	NBSimpleWriter(*plot){
}


//
// Message writer
//
template<typename T>
ConMsg<T>::ConMsg(uint8_t code, const T& payload,
		bool use_dynamic_type) : code(code), payload(payload), dyn_type(use_dynamic_type) {
}

template<typename T>
void ConMsg<T>::toStream(BinaryStream &stream) const {
	stream.write(code);
	if ( dyn_type )
		payload.toStream(stream);
	else
		payload.T::toStream(stream);
}

template<>
void ConMsg<std::string>::toStream(BinaryStream &stream) const {
	stream.write(code);
	stream.write(payload);
}

template<>
void ConMsg<uint32_t>::toStream(BinaryStream &stream) const {
	stream.write(code);
	stream.write(payload);
}


template <typename T>
NBMessageWriter<T>::NBMessageWriter(uint8_t code, const T &payload, bool dyn_type) :
	NBSimpleWriter<ConMsg<T>>( ConMsg<T>(code,payload,dyn_type)) {

}

NBHelloWriter::NBHelloWriter(uint32_t hostid, const std::string& hostname) {
	add_writer(make_unique<NBSimpleWriter<uint8_t>>(ControlConnection::CMD_HELLO));
	add_writer(make_unique<NBSimpleWriter<uint32_t>>(hostid) );
	add_writer(make_unique<NBSimpleWriter<std::string>>(hostname) );
}


template class NBSimpleWriter<uint8_t> ;
template class NBSimpleWriter<MoveInfo> ;

template class NBMessageWriter<uint32_t> ;
template class NBMessageWriter<std::string> ;
template class NBMessageWriter<DeliveryResponse> ;
template class NBMessageWriter<ReorgDescription> ;
template class NBMessageWriter<CacheRef> ;
template class NBMessageWriter<BaseRequest> ;
template class NBMessageWriter<PuzzleRequest> ;

///////////////////////////////////////////////////////////////////
//
//  READER
//
///////////////////////////////////////////////////////////////////

NBReader::~NBReader() {
}

NBReader::NBReader() {
}

std::unique_ptr<BinaryStream> NBReader::get_stream() const {
	if ( is_finished() ) {
		std::unique_ptr<BinaryStream> res = make_unique<StreamBuffer>();
		write_data(*res);
		return res;
	}
	throw IllegalStateException("Can only return stream-buffer when finished reading.");
}

//
// Fixed size reader
//

NBFixedSizeReader::NBFixedSizeReader(size_t len) :
	finished(false), error(false), bytes_read(0), bytes_total(len) {
	data = (unsigned char*) malloc( bytes_total );
	if ( data == nullptr )
		throw OperatorException("Could not malloc buffer-space");
}

NBFixedSizeReader::~NBFixedSizeReader() {
	if ( data != nullptr )
		free(data);
}

void NBFixedSizeReader::read(int fd) {
	if ( error || finished )
			throw IllegalStateException(concat("Illegal state for reading. Error: ", error, ", Finished: ", finished));

	ssize_t currently_read = 0;
	while ( currently_read >= 0 && bytes_read < bytes_total ) {
		currently_read= ::read(fd,data+bytes_read, bytes_total - bytes_read );
		if ( currently_read >= 0 )
			bytes_read += currently_read;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", bytes_read, strerror(errno));
			error = true;
			return;
		}
	}
	finished = true;
}

bool NBFixedSizeReader::has_error() const {
	return error;
}

bool NBFixedSizeReader::is_finished() const {
	return finished;
}

ssize_t NBFixedSizeReader::get_total_read() const {
	return bytes_read;
}

std::string NBFixedSizeReader::to_string() const {
	return concat("FixedSizeReader[bytes_read: ", bytes_read, ", bytes_total: ", bytes_total, ", error: ", error, ", finished: ", finished, "]");
}

void NBFixedSizeReader::write_data(BinaryStream& stream) const {
	if ( !finished )
			throw IllegalStateException("Can only write data when finished reading.");
		stream.write((char*)data,bytes_read);
}

void NBFixedSizeReader::reset() {
	bytes_read = 0;
	finished = false;
	error = false;
}

//
// String reader
//

NBStringReader::NBStringReader() : finished(false), error(false), len(0), len_read(0), data_read(0), data(nullptr) {
}

NBStringReader::~NBStringReader() {
	if ( data != nullptr )
		free(data);
}

void NBStringReader::read(int fd) {
	if ( error || finished )
			throw IllegalStateException(concat("Illegal state for reading. Error: ", error, ", Finished: ", finished));

	char* lp = (char*) &len;

	ssize_t currently_read = 0;
	while ( currently_read >= 0 && len_read < sizeof(len) ) {
		currently_read= ::read(fd, lp+len_read, sizeof(len) - len_read );
		if ( currently_read >= 0 )
			len_read += currently_read;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", len_read, strerror(errno));
			error = true;
			return;
		}
	}

	if ( data == nullptr ) {
		data = (char*) malloc(len);
		if ( data == nullptr) {
			Log::error("Could not alloc space for string to read, required: %ld", len);
			error = true;
			return;
		}
	}

	currently_read = 0;
	while ( currently_read >= 0 && data_read < len ) {
		currently_read= ::read(fd, data+data_read, len - data_read );
		if ( currently_read >= 0 )
			data_read += currently_read;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", len_read + data_read, strerror(errno));
			error = true;
			return;
		}
	}
	finished = true;
}

bool NBStringReader::has_error() const {
	return error;
}

bool NBStringReader::is_finished() const {
	return finished;
}

ssize_t NBStringReader::get_total_read() const {
	return len_read + data_read;
}

std::string NBStringReader::to_string() const {
	return concat("StringReader[bytes_read: ", get_total_read(), ", error: ", error, ", finished: ", finished, "]");
}

void NBStringReader::write_data(BinaryStream& stream) const {
	if ( !finished )
		throw IllegalStateException("Can only write data when finished reading.");
	stream.write((char*)&len,sizeof(len));
	stream.write((char*)data,data_read);
}

void NBStringReader::reset() {
	finished = false;
	error = false;
	if ( data != nullptr ) {
		free(data);
		data = nullptr;
	}
	len = 0;
	len_read = 0;
	data_read = 0;
}

//
// Multi-Reader
//
NBMultiReader::NBMultiReader(std::vector<std::unique_ptr<NBReader> > readers) :
	current_index(0), readers(std::move(readers)) {
	for ( auto &r : this->readers )
		check_reader(*r);
}

NBMultiReader::NBMultiReader() : current_index(0) {
}

void NBMultiReader::read(int fd) {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for reading. Error: ", has_error(), ", Finished: ", is_finished()));

	while ( current_index < readers.size()  ) {
		auto &r = readers.at(current_index);
		r->read(fd);
		if ( r->is_finished() )
			current_index++;
		else
			break;
	}
}

bool NBMultiReader::has_error() const {
	return current_index < readers.size() && readers.at(current_index)->has_error();
}

bool NBMultiReader::is_finished() const {
	return current_index == readers.size();
}

ssize_t NBMultiReader::get_total_read() const {
	ssize_t res(0);
	for ( auto &w : readers ) {
		res += w->get_total_read();
	}
	return res;
}

std::string NBMultiReader::to_string() const {
	return concat("MultiReader[ #readers: ", readers.size(), ", read: ", get_total_read(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}


void NBMultiReader::add_reader(std::unique_ptr<NBReader> r) {
	check_reader(*r);
	readers.push_back(std::move(r));
}

void NBMultiReader::check_reader(const NBReader& r) {
	if ( get_total_read() > 0 || (readers.size() > 0 && (has_error() || is_finished())) )
		throw IllegalStateException("Can only add reader on fresh instance.");
	if ( r.get_total_read() > 0 ||
		 r.has_error() || r.is_finished() )
		throw ArgumentException("Can only build multi-reader with fresh readers.");
}

void NBMultiReader::write_data(BinaryStream& stream) const {
	if ( !is_finished() )
		throw IllegalStateException("Can only write data when finished reading.");
	for ( unsigned int i = 0; i < readers.size(); i++ )
		readers.at(i)->write_data(stream);
}

void NBMultiReader::reset() {
	current_index = 0;
	for ( auto &r : readers )
		r->reset();
}

//
// Container Reader
//
NBContainerReader::NBContainerReader(std::unique_ptr<NBReader> element_reader)
	: element_reader(std::move(element_reader)), size(0), current_index(0), size_read(0),
	  element_read_accum(0), error(false) {
}

void NBContainerReader::read(int fd) {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for reading. Error: ", has_error(), ", Finished: ", is_finished()));

	char* lp = (char*) &size;

	ssize_t currently_read = 0;
	while ( currently_read >= 0 && size_read < sizeof(size) ) {
		currently_read= ::read(fd, lp+size_read, sizeof(size) - size_read );
		if ( currently_read >= 0 )
			size_read += currently_read;
		else if ( errno == EAGAIN || errno == EWOULDBLOCK )
			return;
		else {
			Log::debug("ERROR after %d bytes: %s", size_read, strerror(errno));
			error = true;
			return;
		}
		if ( size_read == sizeof(size) )
			buffer.write((char*) &size, sizeof(size) );
	}

	while ( current_index < size  ) {
		element_reader->read(fd);
		if ( element_reader->is_finished() ) {
			element_read_accum += element_reader->get_total_read();
			element_reader->write_data(buffer);
			element_reader->reset();
			current_index++;

		}
		else
			break;
	}
}

bool NBContainerReader::has_error() const {
	return error || element_reader->has_error();
}

bool NBContainerReader::is_finished() const {
	return size_read == sizeof(uint64_t) && current_index == size;
}

ssize_t NBContainerReader::get_total_read() const {
	ssize_t res = size_read + element_read_accum;
	if ( current_index < size )
		res += element_reader->get_total_read();
	return res;
}

std::string NBContainerReader::to_string() const {
	return concat("ContainerReader");
}

void NBContainerReader::write_data(BinaryStream& stream) const {
	if ( !is_finished() )
		throw IllegalStateException("Can only write data when finished reading.");
	//FIXME: Change to stringstream
	std::string content = buffer.get_content();
	stream.write( content.c_str(), content.length() );
}

void NBContainerReader::reset() {
	buffer.reset();
	element_reader->reset();
	error = false;
	element_read_accum = 0;
	size_read = 0;
	current_index = 0;
	size = 0;
}

NBKVReader::NBKVReader(std::unique_ptr<NBReader> kreader, std::unique_ptr<NBReader> vreader) {
	add_reader( std::move(kreader) );
	add_reader( std::move(vreader) );
}

//
// Node Cache Key
//

NBNodeCacheKeyReader::NBNodeCacheKeyReader() {
	add_reader( make_unique<NBStringReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(uint64_t)) );
}

NBTypedNodeCacheKeyReader::NBTypedNodeCacheKeyReader() {
	add_reader( make_unique<NBStringReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(uint64_t) + sizeof(CacheType)) );
}

//
// QueryRectangle
//

NBQueryRectangleReader::NBQueryRectangleReader() :
	NBFixedSizeReader(sizeof(uint16_t) + 4*sizeof(uint32_t) + 6*sizeof(double)) {
}

//
// BaseRequest
//
NBBaseRequestReader::NBBaseRequestReader() {
	add_reader( make_unique<NBQueryRectangleReader>() );
	add_reader( make_unique<NBStringReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(CacheType)) );
}

//
// ReorgMoveResultReader
//
NBReorgMoveResultReader::NBReorgMoveResultReader() {
	add_reader( make_unique<NBTypedNodeCacheKeyReader>() );
	add_reader( make_unique<NBFixedSizeReader>(2*sizeof(uint32_t) + sizeof(uint64_t)) );
}

NBCapacityReader::NBCapacityReader() : NBFixedSizeReader(10*sizeof(uint64_t)){
}

NBNodeEntryStatsReader::NBNodeEntryStatsReader()
	: NBFixedSizeReader(sizeof(uint64_t) + sizeof(time_t) + sizeof(uint32_t) ){
}

NBCacheStatsReader::NBCacheStatsReader() {
	add_reader( make_unique<NBFixedSizeReader>(sizeof(CacheType)));
	add_reader( make_unique<NBContainerReader>( make_unique<NBKVReader>(
		make_unique<NBStringReader>(),
		make_unique<NBContainerReader>(make_unique<NBNodeEntryStatsReader>()) ) ) );
}

NBNodeStatsReader::NBNodeStatsReader() {
	add_reader( make_unique<NBCapacityReader>() );
	add_reader( make_unique<NBContainerReader>(
					make_unique<NBCacheStatsReader>()
			  ) );
}


NBAccessInfoReader::NBAccessInfoReader() : NBFixedSizeReader(sizeof(time_t) + sizeof(uint32_t)) {
}

NBMoveInfoReader::NBMoveInfoReader() : NBFixedSizeReader(sizeof(time_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(double)) {
}

NBCacheCubeReader::NBCacheCubeReader() :
	NBFixedSizeReader(
		//x,y,t,xres,yres intervals:
		10*sizeof(double) +
		// Actual resolution
		2 *sizeof(double) +
		//EPSG:
		sizeof( epsg_t ) +
		// TIMETYPE
		sizeof( timetype_t ) +
		//RES:
		sizeof( QueryResolution::Type )
	) {
}

NBCacheEntryReader::NBCacheEntryReader() {
	add_reader( make_unique<NBMoveInfoReader>() );
	add_reader( make_unique<NBCacheCubeReader>() );
}

NBNodeCacheRefReader::NBNodeCacheRefReader() {
	add_reader( make_unique<NBTypedNodeCacheKeyReader>() );
	add_reader( make_unique<NBCacheEntryReader>() );
}

NBNodeHandshakeReader::NBNodeHandshakeReader() {
	add_reader( make_unique<NBCapacityReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(uint32_t)) );
	add_reader( make_unique<NBContainerReader>(
		make_unique<NBNodeCacheRefReader>()
	));
}
