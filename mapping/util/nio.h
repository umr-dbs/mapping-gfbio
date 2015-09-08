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
	virtual ssize_t get_total_written() const = 0;
	// Tells the total number of bytes to write
	virtual ssize_t get_total_bytes() const = 0;
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

class NBSimpleWriter : public NBWriter {
public:
	NBSimpleWriter();
	virtual ~NBSimpleWriter();
	void write(int fd);
	bool has_error() const;
	bool is_finished() const;
	ssize_t get_total_written() const;
	std::string to_string() const;
	virtual ssize_t get_total_bytes() const = 0;
protected:
	virtual const unsigned char *get_data() const = 0;
private:
	ssize_t bytes_written;
	bool error;
	bool finished;
};


//
// Writer for primitive types
//

template<typename T>
class NBPrimitiveWriter : public NBSimpleWriter {
public:
	NBPrimitiveWriter( T data );
	virtual ~NBPrimitiveWriter();
	virtual ssize_t get_total_bytes() const;
protected:
	virtual const unsigned char *get_data() const;
private:
	T data;
};

//
// Simple NBWriter -- serializes the object to an internal buffer
// and then writes it to the configured fd
//

template<typename T>
class NBStreamableWriter : public NBSimpleWriter {
public:
	NBStreamableWriter( const T& item, bool use_dynamic_type = false );
	virtual ~NBStreamableWriter();
	virtual ssize_t get_total_bytes() const;
protected:
	virtual const unsigned char *get_data() const;
private:
	std::string data;
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
	virtual ssize_t get_total_written() const;
	virtual ssize_t get_total_bytes() const;
	virtual std::string to_string() const;
protected:
	NBMultiWriter();
	void add_writer( std::unique_ptr<NBWriter> w );
private:
	void check_writer( const NBWriter &writer );
	uint current_index;
	ssize_t total_bytes;
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
	NBRasterWriter( std::shared_ptr<GenericRaster> raster );
private:
	std::shared_ptr<GenericRaster> raster;
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
	ssize_t len_read;

	ssize_t data_read;
	unsigned char* data;
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
	ssize_t size_read;
	ssize_t element_read_accum;
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

class NBCacheStatsReader : public NBContainerReader {
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

class NBCacheBoundsReader : public NBFixedSizeReader {
public:
	NBCacheBoundsReader();
};

class NBNodeCacheRefReader : public NBMultiReader {
public:
	NBNodeCacheRefReader();
};


#endif /* NIO_H_ */
