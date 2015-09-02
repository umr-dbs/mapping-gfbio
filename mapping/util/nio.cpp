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
NBSimpleWriter::NBSimpleWriter() :
	bytes_written(0), error(false), finished(false) {
}

NBSimpleWriter::~NBSimpleWriter() {
}

void NBSimpleWriter::write(int fd) {
	if ( error || finished )
			throw IllegalStateException(concat("Illegal state for writing. Error: ", error, ", Finished: ", finished));

	const unsigned char *data = get_data();

	ssize_t currently_written = 0;
	while ( currently_written >= 0 && bytes_written < get_total_bytes() ) {
		currently_written = ::write(fd,data+bytes_written,get_total_bytes()-bytes_written);
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
	finished = true;
}

bool NBSimpleWriter::has_error() const {
	return error;
}

bool NBSimpleWriter::is_finished() const {
	return finished;
}

ssize_t NBSimpleWriter::get_total_written() const {
	return bytes_written;
}

std::string NBSimpleWriter::to_string() const {
	return concat("SimpleNBWriter[ written: ", bytes_written, ", total: ", get_total_bytes(), ", finished: ", finished, ", error: ", error ,"]");
}


//
// Primitive Writer
//
template<typename T>
NBPrimitiveWriter<T>::NBPrimitiveWriter(T data) : data(data) {
}

template<typename T>
NBPrimitiveWriter<T>::~NBPrimitiveWriter() {
}

template<typename T>
ssize_t NBPrimitiveWriter<T>::get_total_bytes() const {
	return sizeof(data);
}

template<typename T>
const unsigned char* NBPrimitiveWriter<T>::get_data() const {
	return (const unsigned char*) &data;
}

//
// Simple Writer
//

template<typename T>
NBStreamableWriter<T>::NBStreamableWriter(const T& item) {
	StreamBuffer ss;
	BinaryStream &stream = ss;
	stream.write(item);
	data = ss.get_content();
}

template<typename T>
NBStreamableWriter<T>::~NBStreamableWriter() {
}

template<typename T>
ssize_t NBStreamableWriter<T>::get_total_bytes() const {
	return data.size();
}

template<typename T>
const unsigned char* NBStreamableWriter<T>::get_data() const {
	return (const unsigned char*) data.data();
}

//
// Multi Writer
//

NBMultiWriter::NBMultiWriter(std::vector<std::unique_ptr<NBWriter> > writers) :
	current_index(0), total_bytes(0), writers(std::move(writers)) {
	for ( auto &w : this->writers ) {
		check_writer(*w);
		total_bytes += w->get_total_bytes();
	}
}

NBMultiWriter::NBMultiWriter() :
	current_index(0), total_bytes(0) {
}

void NBMultiWriter::add_writer(std::unique_ptr<NBWriter> w) {
	check_writer(*w);
	total_bytes += w->get_total_bytes();
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

ssize_t NBMultiWriter::get_total_written() const {
	ssize_t res(0);
	for ( auto &w : writers ) {
		res += w->get_total_written();
	}
	return res;
}

ssize_t NBMultiWriter::get_total_bytes() const {
	return total_bytes;
}

std::string NBMultiWriter::to_string() const {
	return concat("MultiNBWriter[ #writer: ", writers.size(), ", written: ", get_total_written(), ", total: ", get_total_bytes(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}

//
// Raster-Writer
//

class NBRasterDataWriter : public NBSimpleWriter {
public:
	NBRasterDataWriter( std::shared_ptr<GenericRaster> raster);
	virtual ssize_t get_total_bytes() const;
protected:
	virtual const unsigned char *get_data() const;
private:
	std::shared_ptr<GenericRaster> raster;
};

NBRasterDataWriter::NBRasterDataWriter(std::shared_ptr<GenericRaster>  raster) :
  raster(raster) {
}

ssize_t NBRasterDataWriter::get_total_bytes() const {
	return raster->getDataSize();
}

const unsigned char* NBRasterDataWriter::get_data() const {
	return (unsigned char*) raster->getData();
}

NBRasterWriter::NBRasterWriter( std::shared_ptr<GenericRaster> raster ) {
	add_writer(make_unique<NBStreamableWriter<DataDescription>>( raster->dd ));
	add_writer(make_unique<NBStreamableWriter<SpatioTemporalReference>>( raster->stref ));
	add_writer(make_unique<NBPrimitiveWriter<uint32_t>>( raster->width ));
	add_writer(make_unique<NBPrimitiveWriter<uint32_t>>( raster->height ));
	add_writer(make_unique<NBRasterDataWriter>( raster ));
	add_writer(make_unique<NBStreamableWriter<DirectMetadata<std::string>>>( raster->md_string ));
	add_writer(make_unique<NBStreamableWriter<DirectMetadata<double>>>( raster->md_value ));
}


//
// Message writer
//

NBMessageWriter::NBMessageWriter(uint8_t code, std::unique_ptr<NBWriter> payload) {
	add_writer(make_unique<NBPrimitiveWriter<uint8_t>>(code));
	add_writer(std::move(payload));
}

NBErrorWriter::NBErrorWriter(uint8_t code, const std::string& msg) :
	NBMessageWriter(code, make_unique<NBStreamableWriter<std::string>>(msg)) {
}

template class NBPrimitiveWriter<uint8_t> ;
template class NBPrimitiveWriter<uint32_t> ;
template class NBStreamableWriter<DeliveryResponse> ;
template class NBStreamableWriter<ReorgDescription> ;
template class NBStreamableWriter<CacheRef> ;
template class NBStreamableWriter<BaseRequest> ;
template class NBStreamableWriter<PuzzleRequest> ;

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

	unsigned char* lp = (unsigned char*) &len;

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
		data = (unsigned char*) malloc(len);
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

	unsigned char* lp = (unsigned char*) &size;

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
	element_reader.reset();
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
}

//
// ReorgMoveResultReader
//
NBReorgMoveResultReader::NBReorgMoveResultReader() {
	add_reader( make_unique<NBNodeCacheKeyReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(ReorgMoveResult::Type) + 2*sizeof(uint32_t) + sizeof(uint64_t)) );
}

NBCapacityReader::NBCapacityReader() : NBFixedSizeReader(2*sizeof(uint64_t)){
}

NBNodeEntryStatsReader::NBNodeEntryStatsReader()
	: NBFixedSizeReader(sizeof(uint64_t) + sizeof(time_t) + sizeof(uint32_t) ){
}

NBCacheStatsReader::NBCacheStatsReader() :
	NBContainerReader( make_unique<NBKVReader>(
		make_unique<NBStringReader>(),
		make_unique<NBContainerReader>(make_unique<NBNodeEntryStatsReader>()) ) ) {
}

NBNodeStatsReader::NBNodeStatsReader() {
	add_reader( make_unique<NBCapacityReader>() );
	add_reader( make_unique<NBCacheStatsReader>() );
}

NBCacheBoundsReader::NBCacheBoundsReader() :
	NBFixedSizeReader(
		//SREF:
		sizeof( uint32_t ) + 4*sizeof(double) +
		//TREF:
		sizeof( uint32_t ) + 2*sizeof(double) +
		//RES:
		sizeof( QueryResolution::Type ) + 4*sizeof(double)
	) {
}

NBNodeCacheRefReader::NBNodeCacheRefReader() {
	add_reader( make_unique<NBNodeCacheKeyReader>() );
	add_reader( make_unique<NBCacheBoundsReader>() );
	add_reader( make_unique<NBFixedSizeReader>(sizeof(uint64_t) + sizeof(time_t) + sizeof(uint32_t)) );
}
