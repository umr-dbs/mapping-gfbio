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

std::string StreamBuffer::get_content() {
	return stream.str();
}

//
// Writer interface
//

NBWriter::NBWriter(int fd) : fd(fd) {
}

NBWriter::~NBWriter() {
}


//
// Simple Writer
//
SimpleNBWriter::SimpleNBWriter(int fd) :
	NBWriter(fd), bytes_written(0), error(false), finished(false) {
}

SimpleNBWriter::~SimpleNBWriter() {
}

void SimpleNBWriter::write() {
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

bool SimpleNBWriter::has_error() const {
	return error;
}

bool SimpleNBWriter::is_finished() const {
	return finished;
}

ssize_t SimpleNBWriter::get_total_written() const {
	return bytes_written;
}

std::string SimpleNBWriter::to_string() const {
	return concat("SimpleNBWriter[ written: ", bytes_written, ", total: ", get_total_bytes(), ", finished: ", finished, ", error: ", error ,"]");
}


//
// Primitive Writer
//
template<typename T>
PrimitiveNBWriter<T>::PrimitiveNBWriter(T data, int fd) :
	SimpleNBWriter(fd), data(data) {
}

template<typename T>
PrimitiveNBWriter<T>::~PrimitiveNBWriter() {
}

template<typename T>
ssize_t PrimitiveNBWriter<T>::get_total_bytes() const {
	return sizeof(data);
}

template<typename T>
const unsigned char* PrimitiveNBWriter<T>::get_data() const {
	return (const unsigned char*) &data;
}

//
// Simple Writer
//

template<typename T>
StreamableNBWriter<T>::StreamableNBWriter(const T& item, int fd) :
	SimpleNBWriter(fd) {
	StreamBuffer ss;
	BinaryStream &stream = ss;
	stream.write(item);
	data = ss.get_content();
}

template<typename T>
StreamableNBWriter<T>::~StreamableNBWriter() {
}

template<typename T>
ssize_t StreamableNBWriter<T>::get_total_bytes() const {
	return data.size();
}

template<typename T>
const unsigned char* StreamableNBWriter<T>::get_data() const {
	return (const unsigned char*) data.data();
}

//
// Multi Writer
//

MultiNBWriter::MultiNBWriter(std::vector<std::unique_ptr<NBWriter> > writers, int fd) :
	NBWriter(fd), current_index(0), total_bytes(0), writers(std::move(writers)) {
	for ( auto &w : this->writers ) {
		check_writer(*w);
		total_bytes += w->get_total_bytes();
	}
}

MultiNBWriter::MultiNBWriter(int fd) :
	NBWriter(fd), current_index(0), total_bytes(0) {
}

void MultiNBWriter::add_writer(std::unique_ptr<NBWriter> w) {
	check_writer(*w);
	total_bytes += w->get_total_bytes();
	writers.push_back( std::move(w) );
}

void MultiNBWriter::check_writer(const NBWriter& w) {
	if ( get_total_written() > 0 || (writers.size() > 0 && (has_error() || is_finished())) )
		throw IllegalStateException("Can only add writer on fresh instance.");
	if ( w.get_total_written() > 0 ||
		 w.has_error() || w.is_finished() )
		throw ArgumentException("Can only build multi-writer with fresh writers.");
	if ( w.fd != fd )
		throw ArgumentException("All contained writers must use the same fd.");
}

void MultiNBWriter::write() {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for writing. Error: ", has_error(), ", Finished: ", is_finished()));

	while ( current_index < writers.size()  ) {
		auto &w = writers.at(current_index);
		w->write();
		if ( w->is_finished() )
			current_index++;
		else
			break;
	}
}

bool MultiNBWriter::has_error() const {
	return current_index < writers.size() && writers.at(current_index)->has_error();
}

bool MultiNBWriter::is_finished() const {
	return current_index == writers.size();
}

ssize_t MultiNBWriter::get_total_written() const {
	ssize_t res(0);
	for ( auto &w : writers ) {
		res += w->get_total_written();
	}
	return res;
}

ssize_t MultiNBWriter::get_total_bytes() const {
	return total_bytes;
}

std::string MultiNBWriter::to_string() const {
	return concat("MultiNBWriter[ #writer: ", writers.size(), ", written: ", get_total_written(), ", total: ", get_total_bytes(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}

//
// Raster-Writer
//

class RasterDataWriter : public SimpleNBWriter {
public:
	RasterDataWriter( std::shared_ptr<GenericRaster> raster, int fd);
	virtual ssize_t get_total_bytes() const;
protected:
	virtual const unsigned char *get_data() const;
private:
	std::shared_ptr<GenericRaster> raster;
};

RasterDataWriter::RasterDataWriter(std::shared_ptr<GenericRaster>  raster, int fd) :
  SimpleNBWriter(fd), raster(raster) {
}

ssize_t RasterDataWriter::get_total_bytes() const {
	return raster->getDataSize();
}

const unsigned char* RasterDataWriter::get_data() const {
	return (unsigned char*) raster->getData();
}

NBRasterWriter::NBRasterWriter( std::shared_ptr<GenericRaster> raster, int fd ) :
	MultiNBWriter( fd ) {
	add_writer(make_unique<StreamableNBWriter<DataDescription>>( raster->dd, fd ));
	add_writer(make_unique<StreamableNBWriter<SpatioTemporalReference>>( raster->stref, fd ));
	add_writer(make_unique<PrimitiveNBWriter<uint32_t>>( raster->width, fd ));
	add_writer(make_unique<PrimitiveNBWriter<uint32_t>>( raster->height, fd ));
	add_writer(make_unique<RasterDataWriter>( raster, fd ));
	add_writer(make_unique<StreamableNBWriter<DirectMetadata<std::string>>>( raster->md_string, fd ));
	add_writer(make_unique<StreamableNBWriter<DirectMetadata<double>>>( raster->md_value, fd ));
}


//
// Message writer
//

NBMessageWriter::NBMessageWriter(uint8_t code, std::unique_ptr<NBWriter> payload, int fd) :
	MultiNBWriter(fd) {
	add_writer(make_unique<PrimitiveNBWriter<uint8_t>>(code,fd));
	add_writer(std::move(payload));
}

template class PrimitiveNBWriter<uint8_t> ;

NBErrorWriter::NBErrorWriter(uint8_t code, const std::string& msg, int fd) :
	NBMessageWriter(code, make_unique<StreamableNBWriter<std::string>>(msg,fd), fd) {
}

//
//
//  READER
//
//

NBReader::~NBReader() {
}

NBReader::NBReader(int fd) : fd(fd) {
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

FixedSizeReader::FixedSizeReader(int fd, size_t len) :
	NBReader(fd), finished(false), error(false), bytes_read(0), bytes_total(len) {
	data = (unsigned char*) malloc( bytes_total );
	if ( data == nullptr )
		throw OperatorException("Could not malloc buffer-space");
}

FixedSizeReader::~FixedSizeReader() {
	if ( data != nullptr )
		free(data);
}

void FixedSizeReader::read() {
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

bool FixedSizeReader::has_error() const {
	return error;
}

bool FixedSizeReader::is_finished() const {
	return finished;
}

ssize_t FixedSizeReader::get_total_read() const {
	return bytes_read;
}

std::string FixedSizeReader::to_string() const {
	return concat("FixedSizeReader[bytes_read: ", bytes_read, ", bytes_total: ", bytes_total, ", error: ", error, ", finished: ", finished, "]");
}

void FixedSizeReader::write_data(BinaryStream& stream) const {
	if ( !finished )
			throw IllegalStateException("Can only write data when finished reading.");
		stream.write((char*)data,bytes_read);
}

//
// String reader
//

StringReader::StringReader(int fd) : NBReader(fd), finished(false), error(false), len(0), len_read(0), data_read(0), data(nullptr) {
}

StringReader::~StringReader() {
	if ( data != nullptr )
		free(data);
}

void StringReader::read() {
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
			Log::error("Could not alloc space for string to read");
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

bool StringReader::has_error() const {
	return error;
}

bool StringReader::is_finished() const {
	return finished;
}

ssize_t StringReader::get_total_read() const {
	return len_read + data_read;
}

std::string StringReader::to_string() const {
	return concat("StringReader[bytes_read: ", get_total_read(), ", error: ", error, ", finished: ", finished, "]");
}

void StringReader::write_data(BinaryStream& stream) const {
	if ( !finished )
		throw IllegalStateException("Can only write data when finished reading.");
	stream.write((char*)&len,sizeof(len));
	stream.write((char*)data,data_read);
}

//
// Multi-Reader
//
MultiReader::MultiReader(std::vector<std::unique_ptr<NBReader> > readers, int fd) :
	NBReader(fd), current_index(0), readers(std::move(readers)) {
	for ( auto &r : this->readers )
		check_reader(*r);
}

MultiReader::MultiReader(int fd) : NBReader(fd), current_index(0) {
}

void MultiReader::read() {
	if ( has_error() || is_finished() )
		throw IllegalStateException(concat("Illegal state for reading. Error: ", has_error(), ", Finished: ", is_finished()));

	while ( current_index < readers.size()  ) {
		auto &r = readers.at(current_index);
		r->read();
		if ( r->is_finished() )
			current_index++;
		else
			break;
	}
}

bool MultiReader::has_error() const {
	return current_index < readers.size() && readers.at(current_index)->has_error();
}

bool MultiReader::is_finished() const {
	return current_index == readers.size();
}

ssize_t MultiReader::get_total_read() const {
	ssize_t res(0);
	for ( auto &w : readers ) {
		res += w->get_total_read();
	}
	return res;
}

std::string MultiReader::to_string() const {
	return concat("MultiReader[ #readers: ", readers.size(), ", read: ", get_total_read(), ", finished: ", is_finished(), ", error: ", has_error() ,"]");
}


void MultiReader::add_reader(std::unique_ptr<NBReader> r) {
	check_reader(*r);
	readers.push_back(std::move(r));
}

void MultiReader::check_reader(const NBReader& r) {
	if ( get_total_read() > 0 || (readers.size() > 0 && (has_error() || is_finished())) )
		throw IllegalStateException("Can only add writer on fresh instance.");
	if ( r.get_total_read() > 0 ||
		 r.has_error() || r.is_finished() )
		throw ArgumentException("Can only build multi-writer with fresh writers.");
	if ( r.fd != fd )
		throw ArgumentException("All contained writers must use the same fd.");
}

void MultiReader::write_data(BinaryStream& stream) const {
	if ( !is_finished() )
		throw IllegalStateException("Can only write data when finished reading.");
	for ( unsigned int i = 0; i < readers.size(); i++ )
		readers.at(i)->write_data(stream);
}

//
// Node Cache Key
//

NodeCacheKeyReader::NodeCacheKeyReader(int fd) : MultiReader(fd) {
	add_reader( make_unique<StringReader>(fd) );
	add_reader( make_unique<FixedSizeReader>(fd, sizeof(uint64_t)) );
}


