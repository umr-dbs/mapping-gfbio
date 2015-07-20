#include "converters/converter.h"
#include "converters/raw.h"

#include "raster/profiler.h"

#include <memory>
#include "util/make_unique.h"
#include <bzlib.h>
#include <zlib.h>

static void checkLittleEndian(void)
{
	union {
		uint32_t i32;
		uint8_t i8[4];
	} u = {0x01020304};

	if (u.i8[0] != 4) {
		fprintf(stderr, "Cannot operate on raw buffers on big endian systems, aborting\n");
		exit(5);
	}
}


/**
 * RawConverter: raw buffer, uncompressed
 */
RawConverter::RawConverter() {
	checkLittleEndian();
}

std::unique_ptr<ByteBuffer> RawConverter::encode(GenericRaster *raster) {
	size_t size = raster->getDataSize();
	unsigned char *copy = new unsigned char[size];
	memcpy(copy, raster->getData(), size);

	return make_unique<ByteBuffer>(copy, size);
}

std::unique_ptr<GenericRaster> RawConverter::decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth) {
	auto raster = GenericRaster::create(datadescription, stref, width, height, depth);
	memcpy(raster->getDataForWriting(), buffer.data, buffer.size);
	return raster;
}



/**
 * BzipConverter: raw buffer, compressed
 */
BzipConverter::BzipConverter() {
	checkLittleEndian();
}

std::unique_ptr<ByteBuffer> BzipConverter::encode(GenericRaster *raster) {
	Profiler::Profiler p("Bzip::compress");
	size_t raw_size = raster->getDataSize();
	// per documentation, this amount of space is guaranteed to fit the compressed stream
	unsigned int compressed_size = (unsigned int) (1.1 * raw_size) + 601;
	unsigned char *compressed = new unsigned char[compressed_size];

	int res = BZ2_bzBuffToBuffCompress(
		(char *) compressed, &compressed_size,
		(char *) raster->getData(), raw_size,
		9, 0, 0);

	if (res != BZ_OK) {
		delete [] compressed;
		throw ConverterException("Error on BZ2 compress");
	}

	return make_unique<ByteBuffer>(compressed, compressed_size);
}

std::unique_ptr<GenericRaster> BzipConverter::decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth) {
	//Profiler::Profiler p("Bzip::decompress");
	auto raster = GenericRaster::create(datadescription, stref, width, height, depth);

	char *data = (char *) raster->getDataForWriting();
	unsigned int result_size = raster->getDataSize();
	int res = BZ2_bzBuffToBuffDecompress(
		data, &result_size,
		(char *) buffer.data, buffer.size,
		0, 0
	);

	if (res != BZ_OK || result_size != raster->getDataSize())
		throw SourceException("Error on BZ2 decompress");

	return raster;
}




/**
 * GzipConverter: raw buffer, compressed
 */
GzipConverter::GzipConverter() {
	checkLittleEndian();
}

std::unique_ptr<ByteBuffer> GzipConverter::encode(GenericRaster *raster) {
	Profiler::Profiler p("Gzip::compress");

	z_stream stream;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	if (deflateInit(&stream, /* compression_level = */ 9) != Z_OK)
		throw ConverterException("Error on deflateInit()");

	size_t raw_size = raster->getDataSize();
	// per documentation, this amount of space is guaranteed to fit the compressed stream
	unsigned int compressed_size = (unsigned int) (1.1 * raw_size) + 601;
	std::unique_ptr<unsigned char []> compressed(new unsigned char[compressed_size]);

	stream.avail_in = raw_size;
	stream.next_in = (unsigned char *) raster->getData();
	stream.avail_out = compressed_size;
	stream.next_out = compressed.get();
	if (deflate(&stream, /* flush = */ Z_FINISH) != Z_STREAM_END) {
		deflateEnd(&stream);
		throw ConverterException("Error on deflate()");
	}

	if (stream.avail_in != 0) {
		deflateEnd(&stream);
		throw ConverterException("Error on deflate(), input remains");
	}

	unsigned int real_size = compressed_size - stream.avail_out;
	deflateEnd(&stream);

	return make_unique<ByteBuffer>(compressed.release(), real_size);
}

std::unique_ptr<GenericRaster> GzipConverter::decode(ByteBuffer &buffer, const DataDescription &datadescription, const SpatioTemporalReference &stref, uint32_t width, uint32_t height, uint32_t depth) {
	//Profiler::Profiler p("Gzip::decompress");
	auto raster = GenericRaster::create(datadescription, stref, width, height, depth);

	char *data = (char *) raster->getDataForWriting();
	unsigned int result_size = raster->getDataSize();

	/*
	std::stringstream msg;
	msg << "Gzip::decompress " << buffer->size << " to " << result_size << " Bytes";
	Profiler::Profiler p(msg.str().c_str());
	*/

	z_stream stream;
	memset(&stream, 0, sizeof(stream));
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	stream.avail_in = 0;
	stream.next_in = Z_NULL;
	if (inflateInit(&stream) != Z_OK)
		throw SourceException("Error on inflateInit()");

	stream.next_in = buffer.data;
	stream.avail_in = buffer.size;

	stream.next_out = (unsigned char *) data;
	stream.avail_out = result_size;
	if (inflate(&stream, Z_NO_FLUSH) != Z_STREAM_END) {
		inflateEnd(&stream);
		throw SourceException("Error on inflate()");
	}
	inflateEnd(&stream);

	return raster;
}
