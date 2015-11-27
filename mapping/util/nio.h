/*
 * nio.h
 *
 *  Created on: 13.08.2015
 *      Author: mika
 */

#ifndef NIO_H_
#define NIO_H_

#include "util/binarystream.h"
#include "datatypes/raster.h"
#include "datatypes/pointcollection.h"
#include "datatypes/linecollection.h"
#include "datatypes/polygoncollection.h"
#include "datatypes/plot.h"

#include <vector>
#include <memory>
#include <string>
#include <sstream>

//
// Experimental NIO-Support
//


//
// Helper used to serialize objects to a buffer first
// or buffer bytes from a stream in order to call
// constructors expecting binary streams
//
class StreamBuffer : public BinaryStream {
public:
	virtual void write(const char *buffer, size_t len);
	virtual size_t read(char *buffer, size_t len, bool allow_eof = false);
	void reset();
	std::string get_content() const;
private:
	std::stringstream stream;
};

//
// Classes for non-blocking writes
//

class NBWriter {
public:
	virtual ~NBWriter();
	// Writes data to the given fd, setting the error or finished flag
	virtual void write(int fd) = 0;
	// Tells if an error occured during write
	virtual bool has_error() const = 0;
	// Tells whether this writer finished writing
	virtual bool is_finished() const = 0;
	// Tells the number of bytes written so far
	virtual size_t get_total_written() const = 0;
	// Returns a string-representation of this writer -- logging purposes
	virtual std::string to_string() const = 0;
protected:
	NBWriter();
};

//
// Base-class for writing
// Simply expects implementors to
// be able to tell the size in bytes
// and serve a pointer to the data to write
//

template<typename T>
class NBSimpleWriter : public NBWriter {
public:
	NBSimpleWriter( const T &value, bool use_dynamic_typ = false );
	virtual ~NBSimpleWriter();
	void write(int fd);
	bool has_error() const;
	bool is_finished() const;
	size_t get_total_written() const;
	std::string to_string() const;
	void set_data( const T &value);

private:
	template <typename U = T>
	typename std::enable_if< !std::is_class<U>::value >::type set( const U &val ) {
		if ( data == nullptr ) {
			data = (char*) malloc(sizeof(U));
			total_bytes = sizeof(U);
		}
		memcpy(data, &val, sizeof(U) );
	}

	template <typename U = T>
	typename std::enable_if< std::is_class<U>::value && !std::is_same<U,std::string>::value >::type set( const U &val ) {
		if ( data != nullptr )
			free(data);

		StreamBuffer buf;
		BinaryStream &bs = buf;

		if ( use_dynamic_type )
			val.toStream(bs);
		else
			val.U::toStream(bs);

		auto written = buf.get_content();
		total_bytes = written.length();
		data = (char*) malloc(total_bytes);
		memcpy(data,written.c_str(),total_bytes);
	}

	template <typename U = T>
	typename std::enable_if< std::is_same<U,std::string>::value >::type set( const U &val ) {
		if ( data != nullptr )
			free(data);
		size_t size = val.length();
		total_bytes = size + sizeof(size_t);

		data = (char*) malloc(total_bytes);
		memcpy(data,&size,sizeof(size_t));
		memcpy(data+sizeof(size_t),val.c_str(), size);
	}

	size_t bytes_written;
	size_t total_bytes;
	bool error;
	bool use_dynamic_type;
	char* data;
};

template<typename CType, typename ElementWriter>
class NBContainerWriter : public NBWriter {
public:
	NBContainerWriter( const CType &container, bool write_size = true );
	virtual void write(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual size_t get_total_written() const;
	virtual std::string to_string() const;
private:
	uint64_t element_count;
	bool write_size;
	size_t total_written;
	bool error;
	typename CType::const_iterator iter;
	typename CType::const_iterator end;
	std::unique_ptr<ElementWriter> e_writer;
};

template<typename K, typename V, typename KWriter, typename VWriter>
class NBPairWriter : public NBWriter {
public:
	NBPairWriter( const std::pair<K,V> &p );
	virtual void write(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual size_t get_total_written() const;
	virtual std::string to_string() const;
	void set_data( const std::pair<K,V> &data );
private:
	std::unique_ptr<KWriter> kw;
	std::unique_ptr<VWriter> vw;
};

//
//
// Writer serializing multiple nb-writers in a non-blocking fashion
//
//
class NBMultiWriter : public NBWriter {
public:
	NBMultiWriter( std::unique_ptr<NBWriter> w1, std::unique_ptr<NBWriter> w2 );
	NBMultiWriter( std::vector<std::unique_ptr<NBWriter>> writers );
	virtual void write(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual size_t get_total_written() const;
	virtual std::string to_string() const;
protected:
	NBMultiWriter();
	void add_writer( std::unique_ptr<NBWriter> w );
private:
	void check_writer( const NBWriter &writer );
	uint current_index;
 	std::vector<std::unique_ptr<NBWriter>> writers;
};

//
// Writer sending a message-code and a payload
//

class NBMessageWriter : public NBMultiWriter {
public:
	NBMessageWriter( uint8_t code, std::unique_ptr<NBWriter> payload );
};

//
// Writer sending an error-message
//

class NBErrorWriter : public NBMessageWriter {
public:
	NBErrorWriter( uint8_t code, const std::string &msg );
};


//
// Writer for sending raster-data
//

class NBRasterWriter : public NBMultiWriter {
public:
	NBRasterWriter( std::shared_ptr<const GenericRaster> raster );
};

// TODO: Serialize in a less memory consuming manner
class NBPointsWriter : public NBSimpleWriter<PointCollection> {
public:
	NBPointsWriter( std::shared_ptr<const PointCollection> points );
};

// TODO: Serialize in a less memory consuming manner
class NBLinesWriter : public NBSimpleWriter<LineCollection> {
public:
	NBLinesWriter( std::shared_ptr<const LineCollection> lines );
};

// TODO: Serialize in a less memory consuming manner
class NBPolygonsWriter : public NBSimpleWriter<PolygonCollection> {
public:
	NBPolygonsWriter( std::shared_ptr<const PolygonCollection> polys );
};

// TODO: Serialize in a less memory consuming manner
class NBPlotWriter : public NBSimpleWriter<GenericPlot> {
public:
	NBPlotWriter( std::shared_ptr<const GenericPlot> plot );
};


//
//
//  READER
//
//

class NBReader {
public:
	virtual ~NBReader();
	// Reads data from the given fd, setting the error or finished flag
	virtual void read(int fd) = 0;
	// Tells if an error occured during read
	virtual bool has_error() const = 0;
	// Tells whether this writer finished reading
	virtual bool is_finished() const = 0;
	// Tells the number of bytes read so far
	virtual ssize_t get_total_read() const = 0;
	// Returns a string-representation of this reader -- logging purposes
	virtual std::string to_string() const = 0;
	// Writes the data to the given binary stream
	virtual void write_data( BinaryStream &stream ) const = 0;
	// Returns a binary stream holding the data read from fd
	std::unique_ptr<BinaryStream> get_stream() const;
	// Resets this reader
	virtual void reset() = 0;
protected:
	NBReader();
};

//
// Reader reading a fixed amount of bytes from the
// given fd
//

class NBFixedSizeReader : public NBReader {
public:
	NBFixedSizeReader( size_t len );
	virtual ~NBFixedSizeReader();
	virtual void read(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
	virtual void reset();
private:
	bool finished;
	bool error;
	ssize_t bytes_read;
	ssize_t bytes_total;
	unsigned char* data;
	StreamBuffer sb;
};

//
// Reader for strings
//
class NBStringReader : public NBReader {
public:
	NBStringReader();
	virtual ~NBStringReader();
	virtual void read(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
	virtual void reset();
private:
	bool finished;
	bool error;
	size_t len;
	size_t len_read;

	size_t data_read;
	char* data;
};

//
// Reader serializing reads of multiple readers
//
class NBMultiReader : public NBReader {
public:
	NBMultiReader( std::vector<std::unique_ptr<NBReader>> readers );
	virtual void read(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
	virtual void reset();
protected:
	NBMultiReader();
	void add_reader( std::unique_ptr<NBReader> r );
private:
	void check_reader( const NBReader &reader );
	uint current_index;
	std::vector<std::unique_ptr<NBReader>> readers;
};

class NBContainerReader : public NBReader {
public:
	NBContainerReader( std::unique_ptr<NBReader> element_reader );
	virtual void read(int fd);
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
	virtual void reset();
private:
	std::unique_ptr<NBReader> element_reader;
	uint64_t size;
	uint64_t current_index;
	size_t size_read;
	size_t element_read_accum;
	StreamBuffer buffer;
	bool error;
};

class NBKVReader : public NBMultiReader {
public:
	NBKVReader( std::unique_ptr<NBReader> kreader, std::unique_ptr<NBReader> vreader );
};

///////////////////////////////
//
// DELIVERY CONNECTION
//
///////////////////////////////

//
// Reader for NodeCacheKeys
//
class NBNodeCacheKeyReader : public NBMultiReader {
public:
	NBNodeCacheKeyReader();
};

class NBTypedNodeCacheKeyReader : public NBMultiReader {
public:
	NBTypedNodeCacheKeyReader();
};

///////////////////////////////
//
// CLIENT CONNECTION
//
///////////////////////////////

//
// QueryRectangle
//
class NBQueryRectangleReader : public NBFixedSizeReader {
public:
	NBQueryRectangleReader();
};


//
// Reader for BaseRequests
//
class NBBaseRequestReader : public NBMultiReader {
public:
	NBBaseRequestReader();
};

///////////////////////////////
//
// CONTROL CONNECTION
//
///////////////////////////////

//
// ReorgMoveResult
//
class NBReorgMoveResultReader : public NBMultiReader {
public:
	NBReorgMoveResultReader();
};

class NBCapacityReader : public NBFixedSizeReader {
public:
	NBCapacityReader();
};

class NBNodeEntryStatsReader : public NBFixedSizeReader {
public:
	NBNodeEntryStatsReader();
};

class NBCacheStatsReader : public NBMultiReader {
public:
	NBCacheStatsReader();
};

class NBNodeStatsReader : public NBMultiReader {
public:
	NBNodeStatsReader();
};

class NBAccessInfoReader : public NBFixedSizeReader {
public:
	NBAccessInfoReader();
};

class NBMoveInfoReader : public NBFixedSizeReader {
public:
	NBMoveInfoReader();
};

class NBCacheCubeReader : public NBFixedSizeReader {
public:
	NBCacheCubeReader();
};

class NBCacheEntryReader : public NBMultiReader {
public:
	NBCacheEntryReader();
};

class NBNodeCacheRefReader : public NBMultiReader {
public:
	NBNodeCacheRefReader();
};

class NBNodeHandshakeReader : public NBMultiReader {
public:
	NBNodeHandshakeReader();
};

#endif /* NIO_H_ */
