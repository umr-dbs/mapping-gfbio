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
	std::string get_content();
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
	virtual void write() = 0;
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
	const int fd;
protected:
	NBWriter( int fd );
};

//
// Base-class for writing
// Simply expects implementors to
// be able to tell the size in bytes
// and serve a pointer to the data to write
//

class SimpleNBWriter : public NBWriter {
public:
	SimpleNBWriter( int fd );
	virtual ~SimpleNBWriter();
	void write();
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
class PrimitiveNBWriter : public SimpleNBWriter {
public:
	PrimitiveNBWriter( T data, int fd );
	virtual ~PrimitiveNBWriter();
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
class StreamableNBWriter : public SimpleNBWriter {
public:
	StreamableNBWriter( const T& item, int fd );
	virtual ~StreamableNBWriter();
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
class MultiNBWriter : public NBWriter {
public:
	MultiNBWriter( std::vector<std::unique_ptr<NBWriter>> writers, int fd );
	virtual void write();
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_written() const;
	virtual ssize_t get_total_bytes() const;
	virtual std::string to_string() const;
protected:
	MultiNBWriter( int fd );
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

class NBMessageWriter : public MultiNBWriter {
public:
	NBMessageWriter( uint8_t code, std::unique_ptr<NBWriter> payload, int fd );
};

//
// Writer sending an error-message
//

class NBErrorWriter : public NBMessageWriter {
public:
	NBErrorWriter( uint8_t code, const std::string &msg, int fd );
};


//
// Writer for sending raster-data
//

class NBRasterWriter : public MultiNBWriter {
public:
	NBRasterWriter( std::shared_ptr<GenericRaster> raster, int fd );
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
	virtual void read() = 0;
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
	const int fd;
protected:
	NBReader( int fd );
};

//
// Reader reading a fixed amount of bytes from the
// given fd
//

class FixedSizeReader : public NBReader {
public:
	FixedSizeReader( int fd, size_t len );
	virtual ~FixedSizeReader();
	virtual void read();
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
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
class StringReader : public NBReader {
public:
	StringReader( int fd );
	virtual ~StringReader();
	virtual void read();
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
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
class MultiReader : public NBReader {
public:
	MultiReader( std::vector<std::unique_ptr<NBReader>> readers, int fd );
	virtual void read();
	virtual bool has_error() const;
	virtual bool is_finished() const;
	virtual ssize_t get_total_read() const;
	virtual std::string to_string() const;
	virtual void write_data( BinaryStream &stream ) const;
protected:
	MultiReader( int fd );
	void add_reader( std::unique_ptr<NBReader> r );
private:
	void check_reader( const NBReader &reader );
	uint current_index;
	std::vector<std::unique_ptr<NBReader>> readers;
};


//
// Reader for NodeCacheKeys
//
class NodeCacheKeyReader : public MultiReader {
public:
	NodeCacheKeyReader( int fd );
};

#endif /* NIO_H_ */
